const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';

describe('GCP: DELETE Object', function testSuite() {
    this.timeout(30000);
    let config;
    let gcpClient;

    before(() => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
    });

    afterEach(function afterFn(done) {
        gcpRequestRetry({
            method: 'DELETE',
            bucket: this.currentTest.bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in deleting bucket ${err}\n`);
            } else {
                process.stdout.write('Deleted bucket\n');
            }
            return done(err);
        });
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
                }, err => {
                    if (err) {
                        process.stdout.write(`err in creating object ${err}\n`);
                    } else {
                        process.stdout.write('Created object\n');
                    }
                    return done(err);
                });
            });
        });

        it('should successfully delete object', function testFn(done) {
            async.waterfall([
                next => gcpClient.deleteObject({
                    Bucket: this.test.bucketName,
                    Key: this.test.key,
                }, err => {
                    assert.equal(err, null,
                        `Expected success, got error ${err}`);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'GET',
                    bucket: this.test.bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                }, err => {
                    assert(err);
                    assert.strictEqual(err.statusCode, 404);
                    assert.strictEqual(err.code, 'NoSuchKey');
                    return next();
                }),
            ], err => done(err));
        });
    });

    describe('without existing object in bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `somebucket-${Date.now()}`;
            this.currentTest.key = `nonexistingkey-${Date.now()}`;
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

        it('should return 404 and NoSuchKey', function testFn(done) {
            gcpClient.deleteObject({
                Bucket: this.test.bucketName,
                Key: this.test.key,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 404);
                assert.strictEqual(err.code, 'NoSuchKey');
                return done();
            });
        });
    });
});
