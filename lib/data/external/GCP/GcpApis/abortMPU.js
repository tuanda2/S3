const async = require('async');
const { errors } = require('arsenal');
const { _verifyUploadId, _removeParts } = require('./mpuHelper');
const GcpRequest = require('../GcpRequest');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

function _abortMPU(params, callback) {
    if (!params || !params.Key || !params.UploadId ||
        !params.Bucket || !params.MPU || !params.Overflow) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'err in abortMultipartUpload', error);
        return callback(error);
    }
    const delParams = {
        Bucket: params.Bucket,
        MPU: params.MPU,
        Overflow: params.Overflow,
        Prefix: createMpuKey(params.Key, params.UploadId),
    };
    return async.waterfall([
        next => _verifyUploadId.call(this, {
            Bucket: params.MPU,
            Key: params.Key,
            UploadId: params.UploadId,
        }, next),
        next => _removeParts.call(this, delParams, err => next(err)),
    ], err => callback(err));
}

module.exports = function abortMultipartUpload(params, callback) {
    const req = new GcpRequest(this, _abortMPU, params);
    if (callback && typeof callback === 'function') {
        req.send(callback);
    }
    return req;
};
