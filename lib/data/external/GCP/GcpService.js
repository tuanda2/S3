const googleAuth = require('google-auto-auth');
const AWS = require('aws-sdk');
const { errors } = require('arsenal');
const Service = AWS.Service;
const { gcpAcl, gcpMpu, gcpTagging, gcpJson } = require('./GcpApis');

const GcpSigner = require('./GcpSigner');
const GcpManagedUpload = require('./GcpManagedUpload');

function genAuth(authParams) {
    const authOptions = authParams || {};
    Object.assign(authOptions, {
        scopes: 'https://www.googleapis.com/auth/cloud-platform',
    });
    return googleAuth(authOptions);
}

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01'], {
    _maxConcurrent: 5,
    _maxRetries: 5,
    _jsonAuth: null,
    _authParams: null,

    getToken(callback) {
        if (this._jsonAuth) return this._jsonAuth.getToken(callback);

        if (!this._authParams && this.config.authParams &&
            typeof this.config.authParams === 'object') {
            this.authParams = this.config.authParams;
        }
        this._jsonAuth = genAuth(this._authParams);
        if (this._jsonAuth) {
            return this._jsonAuth.getToken(callback);
        }
        // should never happen, but it all preconditions fails
        // can't generate tokens
        return callback(errors.InternalError.customizeDescription(
            'Unable to create a google authorizer'));
    },

    getSignerClass() {
        return GcpSigner;
    },

    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

    // Implemented APIs
    // Bucket APIs
    putBucket(params, callback) {
        return this.createBucket(params, callback);
    },

    getBucket(params, callback) {
        return this.listObjects(params, callback);
    },

    // Object APIs
    upload(params, callback) {
        const uploader = new GcpManagedUpload(this, params);
        if (typeof callback === 'function') uploader.send(callback);
        return uploader;
    },

    putObjectCopy(params, callback) {
        return this.copyObject(params, callback);
    },

    // TO-DO: Implemented the following APIs
    // Service API
    listBuckets(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listBuckets not implemented'));
    },

    // Bucket APIs
    getBucketLocation(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketLocation not implemented'));
    },

    listObjectVersions(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listObjecVersions not implemented'));
    },

    putBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketWebsite not implemented'));
    },

    getBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketWebsite not implemented'));
    },

    deleteBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucketWebsite not implemented'));
    },

    putBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketCors not implemented'));
    },

    getBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketCors not implemented'));
    },

    deleteBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucketCors not implemented'));
    },

    // Object APIs
    putObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObjectTagging not implemented'));
    },

    deleteObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObjectTagging not implemented'));
    },
});

Object.assign(GCP.prototype, gcpAcl, gcpMpu, gcpTagging, gcpJson);

Object.defineProperty(AWS.apiLoader.services.gcp, '2017-11-01', {
    get: function get() {
        const model = require('./gcp-2017-11-01.api.json');
        return model;
    },
    enumerable: true,
    configurable: true,
});

module.exports = GCP;
