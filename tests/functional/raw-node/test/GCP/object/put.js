const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';

describe('GCP: PUT Object', function testSuite() {
    this.timeout(30000);
    let config;
    let gcpClient;

    before(() => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
    });

    afterEach(function afterFn(done) {
        async.waterfall([
            next => makeGcpRequest({
                method: 'DELETE',
                bucket: this.currentTest.bucketName,
                objectKey: this.currentTest.key,
                authCredentials: config.credentials,
            }, err => {
                if (err) {
                    process.stdout.write(`err in deleting object ${err}\n`);
                } else {
                    process.stdout.write('Deleted object\n');
                }
                return next(err);
            }),
            next => gcpRequestRetry({
                method: 'DELETE',
                bucket: this.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, err => {
                if (err) {
                    process.stdout.write(`err in deleting bucket ${err}\n`);
                } else {
                    process.stdout.write('Deleted bucket\n');
                }
                return next(err);
            }),
        ], err => done(err));
    });

    describe('with existing object in bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `somebucket-${Date.now()}`;
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpRequestRetry({
                method: 'PUT',
                bucket: this.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, err => {
                if (err) {
                    process.stdout.write(`err in creating bucket ${err}\n`);
                } else {
                    process.stdout.write('Created bucket\n');
                }
                makeGcpRequest({
                    method: 'PUT',
                    bucket: this.currentTest.bucketName,
                    objectKey: this.currentTest.key,
                    authCredentials: config.credentials,
                }, (err, res) => {
                    if (err) {
                        process.stdout.write(`err in putting object ${err}\n`);
                    } else {
                        process.stdout.write('Created object\n');
                    }
                    this.currentTest.uploadId =
                        res.headers['x-goog-generation'];
                    return done(err);
                });
            });
        });

        it('should overwrite object', function testFn(done) {
            gcpClient.putObject({
                Bucket: this.test.bucketName,
                Key: this.test.key,
            }, (err, res) => {
                assert.notStrictEqual(res.VersionId, this.test.uploadId);
                return done();
            });
        });
    });

    describe('without existing object in bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `somebucket-${Date.now()}`;
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpRequestRetry({
                method: 'PUT',
                bucket: this.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, err => {
                if (err) {
                    process.stdout.write(`err in creating bucket ${err}\n`);
                } else {
                    process.stdout.write('Created bucket\n');
                }
                return done(err);
            });
        });

        it('should successfully put object', function testFn(done) {
            gcpClient.putObject({
                Bucket: this.test.bucketName,
                Key: this.test.key,
            }, (err, putRes) => {
                assert.equal(err, null,
                    `Expected success, got error ${err}`);
                makeGcpRequest({
                    method: 'GET',
                    bucket: this.test.bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                }, (err, getRes) => {
                    if (err) {
                        process.stdout.write(`err in getting bucket ${err}\n`);
                    } else {
                        process.stdout.write('Retrieved bucket\n');
                    }
                    assert.strictEqual(getRes.headers['x-goog-generation'],
                        putRes.VersionId);
                    return done(err);
                });
            });
        });
    });
});
