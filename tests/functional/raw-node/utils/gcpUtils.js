const { makeGcpRequest } = require('./makeRequest');

function gcpRequestRetry(params, retry, callback) {
    const maxRetries = 4;
    const timeout = Math.pow(2, retry) * 1000;
    return setTimeout(makeGcpRequest, timeout, params, (err, res) => {
        if (err) {
            if (retry <= maxRetries && err.statusCode === 429) {
                return gcpRequestRetry(params, retry + 1, callback);
            }
            return callback(err);
        }
        return callback(null, res);
    });
}

function gcpClientRetry(fn, params, callback, retry = 0) {
    const maxRetries = 4;
    const timeout = Math.pow(2, retry) * 1000;
    return setTimeout(fn, timeout, params, (err, res) => {
        if (err) {
            if (retry <= maxRetries && err.statusCode === 429) {
                return gcpClientRetry(fn, params, callback, retry + 1);
            }
            return callback(err);
        }
        return callback(null, res);
    });
}

/*
<CreateBucketConfiguration>
  <LocationConstraint><location></LocationConstraint>
  <StorageClass><storage class></StorageClass>
</CreateBucketConfiguration>
*/
const regionalLoc = 'us-west1';
const multiRegionalLoc = 'us';
function setBucketClass(storageClass) {
    const locationConstraint =
        storageClass === 'REGIONAL' ? regionalLoc : multiRegionalLoc;
    return '<CreateBucketConfiguration>' +
        `<LocationConstraint>${locationConstraint}</LocationConstraint>` +
        `<StorageClass>${storageClass}</StorageClass>` +
        '</CreateBucketConfiguration>';
}

module.exports = {
    gcpRequestRetry,
    gcpClientRetry,
    setBucketClass,
};
