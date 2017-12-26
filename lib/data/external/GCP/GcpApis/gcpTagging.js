const async = require('async');
const { errors } = require('arsenal');
const GcpRequest = require('../GcpRequest');

function _putObjectTagging(params, callback) {
    if (!params.Tagging) {
        return callback(errors.MissingParameter);
    }
    const taggingParams = Object.assign({}, params);
    taggingParams.Metadata = params.Metadata || {};
    delete taggingParams.VersionId;
    delete taggingParams.Tagging;
    taggingParams.CopySource = `${params.Bucket}/${params.Key}`;
    taggingParams.MetadataDirective = 'REPLACE';
    if (params.Tagging.TagSet.length > 10) {
        return callback(errors.BadRequest
            .customizeDescription('Object tags cannot be greater than 10'));
    }
    const taggingDictionary = {};
    for (let i = 0; i < params.Tagging.TagSet.length; ++i) {
        const { Key: key, Value: value } = params.Tagging.TagSet[i];
        if (key.length > 128) {
            return callback(errors.InvalidTag
                .customizeDescription(
                    'The TagKey you have provided is invalid'));
        }
        if (value.length > 256) {
            return callback(errors.InvalidTag
                .customizeDescription(
                    'The TagValue you have provided is invalid'));
        }
        if (taggingDictionary[key]) {
            return callback(errors.InvalidTag
                .customizeDescription(
                    'Cannot provide multiple Tags with the same key'));
        }
        taggingParams.Metadata[`aws-tag-${key}`] = value;
        taggingDictionary[key] = true;
    }
    return this.copyObject(taggingParams, callback);
}

function _getObjectTagging(params, callback) {
    const taggingParams = {
        Bucket: params.Bucket,
        Key: params.Key,
        VersionId: params.VersionId,
    };
    return async.waterfall([
        next => this.headObject(taggingParams, (err, res) => {
            if (err) {
                return next(err);
            }
            return next(null, res);
        }),
        (resObj, next) => {
            const retObj = {
                VersionId: resObj.VersionId,
                TagSet: [],
            };
            Object.keys(resObj.Metadata).forEach(key => {
                if (key.startsWith('aws-tag-')) {
                    retObj.TagSet.push({
                        Key: key.slice(8),
                        Value: resObj.Metadata[key],
                    });
                }
            });
            return next(null, retObj);
        },
    ], (err, result) => {
        if (err) {
            return callback(err);
        }
        return callback(null, result);
    });
}

function _deleteObjectTagging(params, callback) {
    const taggingParams = {
        Bucket: params.Bucket,
        Key: params.Key,
        VersionId: params.VersionId,
    };
    return async.waterfall([
        next => this.headObject(taggingParams, (err, res) => {
            if (err) {
                return next(err);
            }
            return next(null, res);
        }),
        (resObj, next) => {
            const retObj = {
                VersionId: resObj.VersionId,
                Metadata: {},
            };
            Object.keys(resObj.Metadata).forEach(key => {
                if (!key.startsWith('aws-tag-')) {
                    retObj.Metadata[key] = resObj.Metadata[key];
                }
            });
            return next(null, retObj);
        },
    ], (err, result) => {
        if (err) {
            return callback(err);
        }
        const taggingParams = {
            Bucket: params.Bucket,
            Key: params.Key,
            CopySource: `${params.Bucket}/${params.Key}`,
            MetadataDirective: 'REPLACE',
            Metadata: result.Metadata,
        };
        return this.copyObject(taggingParams, callback);
    });
}

module.exports = {
    putObjectTagging(params, callback) {
        const req = new GcpRequest(this, _putObjectTagging, params);
        if (callback && typeof callback === 'function') {
            req.send(callback);
        }
        return req;
    },
    getObjectTagging(params, callback) {
        const req = new GcpRequest(this, _getObjectTagging, params);
        if (callback && typeof callback === 'function') {
            req.send(callback);
        }
        return req;
    },
    deleteObjectTagging(params, callback) {
        const req = new GcpRequest(this, _deleteObjectTagging, params);
        if (callback && typeof callback === 'function') {
            req.send(callback);
        }
        return req;
    },
};
