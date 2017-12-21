const assert = require('assert');
const async = require('async');
const xml2js = require('xml2js');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const credentialTwo = 'gcpbackend2';

const aclExpectedObject = {
    Scope: [{
        $: { type: 'AllUsers' } },
    ],
    Permission: ['READ'],
};
const bucketName = `somebucket-${Date.now()}`;

function parseRespAndAssert(xml, callback) {
    return xml2js.parseString(xml, (err, res) => {
        if (err) {
            process.stdout.write(`err in parsing response ${err}\n`);
            return callback(err);
        }
        const isTrue = res.AccessControlList.Entries[0].Entry.reduce(
        (retBool, curEntry) => {
            let curBool;
            try {
                assert.deepStrictEqual(curEntry, aclExpectedObject);
                curBool = true;
            } catch (err) {
                curBool = false;
            }
            return retBool || curBool;
        }, false);
        assert.strictEqual(isTrue, true);
        return callback();
    });
}

describe('GCP: PUT Object ACL ', function testSuite() {
    this.timeout(30000);
    let config;
    let gcpClient;
    let gcpClient2;

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        const config2 = getRealAwsConfig(credentialTwo);
        gcpClient2 = new GCP(config2);
        gcpRequestRetry({
            method: 'PUT',
            bucket: bucketName,
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

    beforeEach(function beforeFn(done) {
        this.currentTest.key = `somekey-${Date.now()}`;
        makeGcpRequest({
            method: 'PUT',
            bucket: bucketName,
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

    afterEach(function afterFn(done) {
        makeGcpRequest({
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
            return done(err);
        });
    });

    after(done => {
        gcpRequestRetry({
            method: 'DELETE',
            bucket: bucketName,
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

    describe('when user does not have ACL permissions', () => {
        it('should return 403 and AccessDenied', function testFn(done) {
            gcpClient2.putObjectAcl({
                Bucket: bucketName,
                Key: this.test.key,
                ACL: 'public-read-write',
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 403);
                assert.strictEqual(err.code, 'AccessDenied');
                return done();
            });
        });
    });

    describe('when user has ACL permissions', () => {
        it('should put credentials with acl header', function testFn(done) {
            return async.waterfall([
                next => gcpClient.putObjectAcl({
                    Bucket: bucketName,
                    Key: this.test.key,
                    ACL: 'public-read',
                    AccessControlPolicy: {},
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got err ${err}`);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'GET',
                    bucket: bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                    queryObj: { acl: {} },
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving bucket ACL ${err}`);
                        return next(err);
                    }
                    return parseRespAndAssert(res.body, next);
                }),
            ], err => done(err));
        });

        it('should put credentials with body', function testFn(done) {
            return async.waterfall([
                next => gcpClient.putObjectAcl({
                    Bucket: bucketName,
                    Key: this.test.key,
                    AccessControlPolicy: {
                        Grants: [
                            {
                                Grantee: {
                                    Type: 'AllUsers',
                                },
                                Permission: 'READ',
                            },
                        ],
                    },
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got err ${err}`);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'GET',
                    bucket: bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                    queryObj: { acl: {} },
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving bucket ACL ${err}`);
                        return next(err);
                    }
                    return parseRespAndAssert(res.body, next);
                }),
            ], err => done(err));
        });

        it('should put credentials with grant header ', function testFn(done) {
            return async.waterfall([
                next => gcpClient.putObjectAcl({
                    Bucket: bucketName,
                    Key: this.test.key,
                    GrantRead: 'AllUsers',
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got err ${err}`);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'GET',
                    bucket: bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                    queryObj: { acl: {} },
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving bucket ACL ${err}`);
                        return next(err);
                    }
                    return parseRespAndAssert(res.body, next);
                }),
            ], err => done(err));
        });
    });
});
