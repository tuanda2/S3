const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${Date.now()}`;

describe('GCP: COPY Object', function testSuite() {
    this.timeout(8000);
    let config;
    let gcpClient;

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);

        makeGcpRequest({
            method: 'PUT',
            bucket: bucketName,
            authCredentials: config.credentials,
        }, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err}\n`);
            } else {
                process.stdout.write('Created bucket\n');
            }
            return done(err);
        });
    });

    after(done => {
        makeGcpRequest({
            method: 'DELETE',
            bucket: bucketName,
            authCredentials: config.credentials,
        }, err => {
            if (err) {
                process.stdout.write(`err in deleting bucket ${err}\n`);
            } else {
                process.stdout.write('Deleted bucket\n');
            }
            return done(err);
        });
    });

    describe('without existing object in bucket', () => {
        it('should return 404 and NoSuchKey', done => {
            const missingObject = `nonexistingkey-${Date.now()}`;
            const someKey = `somekey-${Date.now()}`;
            gcpClient.copyObject({
                Bucket: bucketName,
                Key: someKey,
                CopySource: `/${bucketName}/${missingObject}`,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 404);
                assert.strictEqual(err.code, 'NoSuchKey');
                return done();
            });
        });
    });

    describe('with existing object in bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            this.currentTest.copyKey = `copykey-${Date.now()}`;
            this.currentTest.initValue = `${Date.now()}`;
            makeGcpRequest({
                method: 'PUT',
                bucket: bucketName,
                objectKey: this.currentTest.copyKey,
                headers: {
                    'x-goog-meta-value': this.currentTest.initValue,
                },
                authCredentials: config.credentials,
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in creating object ${err}\n`);
                } else {
                    process.stdout.write('Created object\n');
                }
                this.currentTest.contentHash = res.headers['x-goog-hash'];
                return done(err);
            });
        });

        afterEach(function afterFn(done) {
            async.parallel([
                next => makeGcpRequest({
                    method: 'DELETE',
                    bucket: bucketName,
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
                next => makeGcpRequest({
                    method: 'DELETE',
                    bucket: bucketName,
                    objectKey: this.currentTest.copyKey,
                    authCredentials: config.credentials,
                }, err => {
                    if (err) {
                        process.stdout
                            .write(`err in deleting copy object ${err}\n`);
                    } else {
                        process.stdout.write('Deleted copy object\n');
                    }
                    return next(err);
                }),
            ], err => done(err));
        });

        it('should successfully copy with REPLACE directive',
        function testFn(done) {
            const newValue = `${Date.now()}`;
            async.waterfall([
                next => gcpClient.copyObject({
                    Bucket: bucketName,
                    Key: this.test.key,
                    CopySource: `/${bucketName}/${this.test.copyKey}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        value: newValue,
                    },
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got error ${err}`);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'HEAD',
                    bucket: bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving object ${err}\n`);
                        return next(err);
                    }
                    process.stdout.write('Retrieved object\n');
                    assert.strictEqual(this.test.contentHash,
                        res.headers['x-goog-hash']);
                    assert.notStrictEqual(res.headers['x-goog-meta-value'],
                        this.test.initValue);
                    return next();
                }),
            ], err => done(err));
        });

        it('should successfully copy with COPY directive',
        function testFn(done) {
            async.waterfall([
                next => gcpClient.copyObject({
                    Bucket: bucketName,
                    Key: this.test.key,
                    CopySource: `/${bucketName}/${this.test.copyKey}`,
                    MetadataDirective: 'COPY',
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got error ${err}`);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'HEAD',
                    bucket: bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving object ${err}\n`);
                        return next(err);
                    }
                    process.stdout.write('Retrieved object\n');
                    assert.strictEqual(this.test.contentHash,
                        res.headers['x-goog-hash']);
                    assert.strictEqual(res.headers['x-goog-meta-value'],
                        this.test.initValue);
                    return next();
                }),
            ], err => done(err));
        });
    });
});
