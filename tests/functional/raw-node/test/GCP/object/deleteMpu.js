const assert = require('assert');
const async = require('async');
const { GCP, GcpUtils } = require('../../../../../../lib/data/external/GCP');
const { gcpRequestRetry, setBucketClass } = require('../../../utils/gcpUtils');
const { minimumAllowedPartSize } = require('../../../../../../constants');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: `somebucket-${Date.now()}`,
        Type: 'MULTI_REGIONAL',
    },
    mpu: {
        Name: `mpubucket-${Date.now()}`,
        Type: 'REGIONAL',
    },
    overflow: {
        Name: `overflowbucket-${Date.now()}`,
        Type: 'MULTI_REGIONAL',
    },
};
const numParts = 10;

function gcpMpuSetup(gcpClient, key, partCount, callback) {
    return async.waterfall([
        next => gcpClient.createMultipartUpload({
            Bucket: bucketNames.mpu.Name,
            Key: key,
        }, (err, res) => {
            assert.equal(err, null,
                `Expected success, but got error ${err}`);
            return next(null, res.UploadId);
        }),
        (uploadId, next) => {
            if (partCount <= 0) {
                return next('SkipPutPart', uploadId);
            }
            const arrayData = Array.from(Array(partCount).keys());
            return async.each(arrayData,
            (info, moveOn) => {
                gcpClient.uploadPart({
                    Bucket: bucketNames.mpu.Name,
                    Key: key,
                    UploadId: uploadId,
                    PartNumber: info + 1,
                    Body: Buffer.alloc(minimumAllowedPartSize, 'a'),
                    ContentLength: minimumAllowedPartSize,
                }, err => moveOn(err));
            }, err => next(err, uploadId));
        },
    ], (err, uploadId) => {
        if (err) {
            if (err === 'SkipPutPart') {
                return callback(null, uploadId);
            }
            return callback(err);
        }
        return callback(null, uploadId);
    });
}

describe('GCP: Abort MPU', function testSuite() {
    this.timeout(30000);
    let config;
    let gcpClient;

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        async.eachSeries(bucketNames,
            (bucket, next) => gcpRequestRetry({
                method: 'PUT',
                bucket: bucket.Name,
                authCredentials: config.credentials,
                requestBody: setBucketClass(bucket.Type),
            }, 0, err => {
                if (err) {
                    process.stdout.write(`err in creating bucket ${err}\n`);
                }
                return next(err);
            }),
        err => done(err));
    });

    after(done => {
        async.eachSeries(bucketNames,
            (bucket, next) => gcpClient.listObjects({
                Bucket: bucket.Name,
            }, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got error ${err}`);
                async.map(res.Contents, (object, moveOn) => {
                    const deleteParams = {
                        Bucket: bucket.Name,
                        Key: object.Key,
                    };
                    gcpClient.deleteObject(
                        deleteParams, err => moveOn(err));
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got error ${err}`);
                    gcpRequestRetry({
                        method: 'DELETE',
                        bucket: bucket.Name,
                        authCredentials: config.credentials,
                    }, 0, err => {
                        if (err) {
                            process.stdout.write(
                                `err in deleting bucket ${err}\n`);
                        }
                        return next(err);
                    });
                });
            }),
        err => done(err));
    });

    describe('when MPU has 0 parts', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpMpuSetup(gcpClient, this.currentTest.key, 0,
            (err, uploadId) => {
                assert.equal(err, null,
                    `Unable to setup MPU test, error ${err}`);
                this.currentTest.uploadId = uploadId;
                return done();
            });
        });

        it('should abort MPU with 0 parts', function testFn(done) {
            return async.waterfall([
                next => {
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Overflow: bucketNames.overflow.Name,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                    };
                    gcpClient.abortMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        return next();
                    });
                },
                next => {
                    const keyName =
                        `${this.test.key}-${this.test.uploadId}/init`;
                    gcpClient.headObject({
                        Bucket: bucketNames.mpu.Name,
                        Key: keyName,
                    }, err => {
                        assert(err);
                        assert.strictEqual(err.code, 404);
                        return next();
                    });
                },
            ], err => done(err));
        });
    });

    describe('when MPU is incomplete', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpMpuSetup(gcpClient, this.currentTest.key, numParts,
            (err, uploadId) => {
                assert.equal(err, null,
                    `Unable to setup MPU test, error ${err}`);
                this.currentTest.uploadId = uploadId;
                return done();
            });
        });

        it('should abort incomplete MPU', function testFn(done) {
            return async.waterfall([
                next => {
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Overflow: bucketNames.overflow.Name,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                    };
                    gcpClient.abortMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        return next();
                    });
                },
                next => {
                    const keyName =
                        `${this.test.key}-${this.test.uploadId}/init`;
                    gcpClient.headObject({
                        Bucket: bucketNames.mpu.Name,
                        Key: keyName,
                    }, err => {
                        assert(err);
                        assert.strictEqual(err.code, 404);
                        return next();
                    });
                },
            ], err => done(err));
        });
    });

    describe('when MPU has been completed', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            async.waterfall([
                next => gcpMpuSetup(gcpClient, this.currentTest.key, numParts,
                (err, uploadId) => {
                    assert.equal(err, null,
                        `Unable to setup MPU test, error ${err}`);
                    this.currentTest.uploadId = uploadId;
                    return next();
                }),
                next => {
                    const parts = GcpUtils.createMpuList({
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                    }, 'parts', numParts);
                    const params = {
                        Bucket: bucketNames.main.Name,
                        MPU: bucketNames.mpu.Name,
                        Overflow: bucketNames.overflow.Name,
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                        MultipartUpload: {
                            Parts: parts,
                        },
                    };
                    gcpClient.completeMultipartUpload(params, err => {
                        assert.equal(err, null,
                            `Unable to complete MPU test, error ${err}`);
                        return next();
                    });
                },
            ], err => done(err));
        });

        it('should return error', function testFn(done) {
            const params = {
                Bucket: bucketNames.main.Name,
                MPU: bucketNames.mpu.Name,
                Overflow: bucketNames.overflow.Name,
                Key: this.test.key,
                UploadId: this.test.uploadId,
            };
            gcpClient.abortMultipartUpload(params, err => {
                assert(err);
                assert.strictEqual(err.code, 404);
                return done();
            });
        });
    });
});
