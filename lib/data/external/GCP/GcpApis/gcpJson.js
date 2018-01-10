const async = require('async');
const request = require('request');
const randomstring = require('randomstring');
const { errors } = require('arsenal');

const { logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');
const GcpRequest = require('../GcpRequest');

const boundaryLength = 16;
const gcpJsonEndpoint = 'https://www.googleapis.com/storage/v1';

/**
 * rewriteObject - copy object between buckets of different storage class or
 * regions. As copyObject has incosistent results when performed on large
 * objects across different buckets
 * @param {object} params - JSON request parameters
 * @param {string} params.SourceBucket - copy source bucket
 * @param {string} params.SourceObject - copy source object
 * @param {string} params.SourceVersionId - specify source version
 * @param {string} params.DestinationBucket - copy destination bucket
 * @param {string} params.DestinationObject - copy destination object
 * @param {string} param.RewriteToken - token to pick up where previous rewrite
 * had left off
 * @param {string} params.Token - If not given, a token will be generated
 * @param {function} callback - callback function
 * @return {(Error|object)} - return Error if rewrite fails, otherwise an object
 * containing completed rewrite results
 */
function _rewriteObject(params, callback) {
    async.waterfall([
        next => {
            if (params.Token) next(null, params.Token);
            else this.getToken((err, res) => next(err, res));
        },
        (token, next) => {
            const uri = `/b/${encodeURIComponent(params.SourceBucket)}` +
                        `/o/${encodeURIComponent(params.SourceObject)}` +
                        '/rewriteTo' +
                        `/b/${encodeURIComponent(params.DestinationBucket)}` +
                        `/o/${encodeURIComponent(params.DestinationObject)}`;
            const qs = {
                sourceGeneration: params.SourceVersionId,
                rewriteToken: params.RewriteToken,
            };
            request({ method: 'POST', baseUrl: gcpJsonEndpoint, uri, qs,
                auth: { bearer: token } },
            (err, resp, body) => {
                let res;
                try {
                    res = JSON.parse(body);
                } catch (err) { res = undefined; }
                if (res && res.error && res.error.code >= 300) {
                    logHelper(logger, 'error',
                        'rewritObject: unable to find',
                        errors.AccessForbidden.customizeDescription(
                            'rewrite object failure'));
                    return next(errors.AccessForbidden);
                }
                return next(err, res);
            });
        },
    ], (err, result) => callback(err, result));
}

function formBatchRequest(bucket, deleteList) {
    let retBody = '';
    const boundary = randomstring.generate(boundaryLength);

    deleteList.forEach(object => {
        // add boundary
        retBody += `--${boundary}\n`;
        // add req headers
        retBody += `Content-Type: application/http\n`;
        retBody += '\n';
        const key = object.Key;
        const versionId = object.VersionId;
        let path = `/storage/v1/b/${bucket}/o/${encodeURIComponent(key)}`;
        if (versionId) path += `?generation=${versionId}`;
        retBody += `DELETE ${path} HTTP/1.1\n`;
        retBody += '\n';
    });
    retBody += `--${boundary}\n`;
    return { body: retBody, boundary };
}

/**
 * deleteObjects - delete a list of objects
 * @param {object} params - deleteObjects parameters
 * @param {string} params.Bucket - bucket location
 * @param {object} params.Delete - delete config object
 * @param {object[]} params.Delete.Objects - a list of objects to be deleted
 * @param {string} params.Delete.Objects[].Key - object key
 * @param {string} params.Delete.Objects[].VersionId - object version Id, if
 * not given the master version will be archived
 * @param {string} params.Token - If not given, a token will be generated
 * @param {function} callback - callback function
 * @return {(Error|string)} - return Error is delete objects batch
 * request fails, otherwise, a string if batch requests succeeds
 */
function _deleteObjects(params, callback) {
    if (!params || !params.Delete || !params.Delete.Objects) {
        return callback(errors.MalformedXML);
    }
    return async.waterfall([
        next => {
            if (params.Token) next(null, params.Token);
            else this.getToken((err, res) => next(err, res));
        },
        (token, next) => {
            const { body, boundary } =
                formBatchRequest(params.Bucket, params.Delete.Objects, token);
            request({
                method: 'POST',
                baseUrl: 'https://www.googleapis.com',
                uri: '/batch',
                headers: {
                    'Content-Type': `multipart/mixed; boundary=${boundary}`,
                },
                body,
                auth: { bearer: token },
            }, (err, resp, body) => {
                // attempt to parse response body
                // if body element can be transformed into an object
                // there then check if the response is a error object
                // TO-DO: maybe, check individual batch op response
                let res;
                try {
                    res = JSON.parse(body);
                } catch (err) { res = undefined; }
                if (typeof res === 'object') {
                    if (res && res.error && res.error.code >= 300) {
                        return next(errors.NotFound);
                    }
                    return next(err);
                }
                return next(err, res);
            });
        },
    ], (err, result) => callback(err, result));
}

module.exports = {
    rewriteObject(params, callback) {
        const req = new GcpRequest(this, _rewriteObject, params);
        if (callback && typeof callback === 'function') {
            req.send(callback);
        }
        return req;
    },
    deleteObjects(params, callback) {
        const req = new GcpRequest(this, _deleteObjects, params);
        if (callback && typeof callback === 'function') {
            req.send(callback);
        }
        return req;
    },
};
