const async = require('async');
const { errors } = require('arsenal');
const { eachSlice, getRandomInt, createMpuKey, logger } =
    require('../GcpUtils');
const { logHelper } = require('../../utils');

function createDelSlices(list) {
    const retSlice = [];
    for (let ind = 0; ind < list.length; ind += 1000) {
        retSlice.push(list.slice(ind, ind + 1000));
    }
    return retSlice;
}

function _retryCompose(params, retry, callback) {
    // retries up each request to a maximum of 5 times before
    // declaring as a failed completeMPU
    const timeout = Math.pow(2, retry) * 1000 + getRandomInt(100, 500);
    return setTimeout((params, callback) =>
    this.composeObject(params, callback), timeout, params, (err, res) => {
        if (err) {
            if (retry <= this._maxRetries && err.statusCode === 429) {
                logger.trace('retryCompose: slow down request',
                    { retryCount: retry, timeout });
                return _retryCompose.call(this, params, retry + 1, callback);
            }
            logHelper(logger, 'error', 'retryCompose: failed to compose', err);
            return callback(err);
        }
        return callback(null, res);
    });
}

function _retryCopy(params, retry, callback) {
    const timeout = Math.pow(2, retry) * 1000 + getRandomInt(100, 500);
    return setTimeout((params, callback) =>
    this.copyObject(params, callback), timeout, params, (err, res) => {
        if (err) {
            if (retry <= this._maxRetries && err.statusCode === 429) {
                logger.trace('retryCopy: slow down request',
                    { retryCount: retry, timeout });
                return _retryCopy.call(this, params, retry + 1, callback);
            }
            logHelper(logger, 'error', 'retryCopy: failed to copy', err);
            return callback(err);
        }
        return callback(null, res);
    });
}

function _splitMerge(params, partList, level, callback) {
    // create composition of slices from the partList array
    return async.mapLimit(eachSlice.call(partList, 32), this._maxConcurrent,
    (infoParts, cb) => {
        const mpuPartList = infoParts.Parts.map(item =>
            ({ PartName: item.PartName }));
        const partNumber = infoParts.PartNumber;
        const tmpKey =
            createMpuKey(params.Key, params.UploadId, partNumber, level);
        const mergedObject = { PartName: tmpKey };
        if (mpuPartList.length < 2) {
            logger.trace('splitMerge: parts are fewer than 2, copy instead');
            // else just perform a copy
            const copyParams = {
                Bucket: params.MPU,
                Key: tmpKey,
                CopySource: `${params.MPU}/${mpuPartList[0].PartName}`,
            };
            return this.copyObject(copyParams, (err, res) => {
                if (err) {
                    logHelper(logger, 'error',
                        'err in splitMerge - copyObject', err);
                    return cb(err);
                }
                mergedObject.VersionId = res.VersionId;
                mergedObject.ETag = res.ETag;
                return cb(null, mergedObject);
            });
        }
        const composeParams = {
            Bucket: params.MPU,
            Key: tmpKey,
            MultipartUpload: { Parts: mpuPartList },
        };
        return _retryCompose.call(this, composeParams, 0, (err, res) => {
            if (err) {
                return cb(err);
            }
            mergedObject.VersionId = res.VersionId;
            mergedObject.ETag = res.ETag;
            return cb(null, mergedObject);
        });
    }, (err, res) => {
        if (err) {
            return callback(err);
        }
        return callback(null, res.length);
    });
}

function _removeParts(params, callback) {
    /**
     * _getObject - retrieves all parts, live and archived to be deleted for
     * abortMultipartUpload or completeMultipartUpload
     * @param {string} bucketType - 'MPU' or 'Overflow'
     * @param {function} callback - callback function
     * @return {(Error|array)} - returns Error if listVersion fails, otherwise,
     * a list of parts to be deleted
     */
    const _getObjectVersions = (bucketType, callback) => {
        logger.trace(`remove all parts ${bucketType} bucket`);
        let partList = [];
        let isTruncated = true;
        let nextMarker;
        const bucket = params[bucketType];
        return async.whilst(() => isTruncated, moveOn => {
            const listParams = {
                Bucket: bucket,
                Prefix: params.Prefix,
                Marker: nextMarker,
            };
            return this.listVersions(listParams, (err, res) => {
                if (err) {
                    logHelper(logger, 'error',
                        `err in removeParts - listVersions ${bucketType}`, err);
                    return moveOn(err);
                }
                nextMarker = res.NextMarker;
                isTruncated = res.IsTruncated;
                partList = partList.concat(res.Versions);
                return moveOn();
            });
        }, err => callback(err, partList));
    };

    /**
     * _deleteObjects - deletes a given list of objects
     * @param {string} bucketType - 'MPU' or 'Overflow'
     * @param {object[]} partsList - a array of objects with Key and VersionId
     * @param {function} callback - callback function
     * @return {(Error)} - returns Error if deleteObjects call fails, otherwise,
     * nothing
     */
    const _deleteObjects = (bucketType, partsList, callback) => {
        logger.trace(`successfully listed ${bucketType} parts`, {
            objectCount: partsList.length,
        });
        const delSlices = createDelSlices(partsList);
        const bucket = params[bucketType];
        return async.each(delSlices, (list, moveOn) => {
            const delParams = {
                Bucket: bucket,
                Delete: { Objects: list },
            };
            return this.deleteObjects(delParams, err => {
                if (err) {
                    logHelper(logger, 'error',
                        `error deleting ${bucketType} object`, err);
                }
                return moveOn(err);
            });
        }, err => callback(err));
    };

    return async.parallel([
        done => async.waterfall([
            next => _getObjectVersions('MPU', next),
            (parts, next) => _deleteObjects('MPU', parts, next),
        ], err => done(err)),
        done => async.waterfall([
            next => _getObjectVersions('Overflow', next),
            (parts, next) => _deleteObjects('Overflow', parts, next),
        ], err => done(err)),
    ], err => callback(err));
}

function _verifyUploadId(params, callback) {
    return this.headObject({
        Bucket: params.Bucket,
        Key: createMpuKey(params.Key, params.UploadId, 'init'),
    }, err => {
        if (err) {
            if (err.statusCode === 404) {
                logHelper(logger, 'error', 'Unable to find MPU init objects',
                    errors.NoSuchUpload);
                return callback(errors.NoSuchUpload);
            }
            logHelper(logger, 'error', 'err in verifyUpload - headObject', err);
            return callback(err);
        }
        return callback();
    });
}

module.exports = {
    _retryCompose,
    _retryCopy,
    _splitMerge,
    _removeParts,
    _verifyUploadId,
};
