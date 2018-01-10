const uuid = require('uuid/v4');
const { errors } = require('arsenal');
const GcpRequest = require('../GcpRequest');
const { createMpuKey, logger } = require('../GcpUtils');
const { logHelper } = require('../../utils');

function _createMPU(params, callback) {
    // As google cloud does not have a create MPU function,
    // create an empty 'init' object that will temporarily store the
    // object metadata and return an upload ID to mimic an AWS MPU
    if (!params || !params.Bucket || !params.Key) {
        const error = errors.InvalidRequest
            .customizeDescription('Missing required parameter');
        logHelper(logger, 'error', 'err in createMultipartUpload', error);
        return callback(error);
    }
    const uploadId = uuid().replace(/-/g, '');
    const mpuParams = {
        Bucket: params.Bucket,
        Key: createMpuKey(params.Key, uploadId, 'init'),
        Metadata: params.Metadata,
        ContentType: params.ContentType,
        CacheControl: params.CacheControl,
        ContentDisposition: params.ContentDisposition,
        ContentEncoding: params.ContentEncoding,
    };
    return this.putObject(mpuParams, err => {
        if (err) {
            logHelper(logger, 'error', 'err in createMPU - putObject', err);
            return callback(err);
        }
        return callback(null, { UploadId: uploadId });
    });
}

module.exports = function createMultipartUpload(params, callback) {
    const req = new GcpRequest(this, _createMPU, params);
    if (callback && typeof callback === 'function') {
        req.send(callback);
    }
    return req;
};
