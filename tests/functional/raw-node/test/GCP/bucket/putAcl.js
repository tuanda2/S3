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

const projectGroupId = 'project-owners-838823050576';
const projectGroupName = 'Project 838823050576 owners';
const aclExpectedObject = {
    AccessControlList: {
        Owner: [{
            ID: [projectGroupId],
            Name: [projectGroupName],
        }],
        Entries: [
            { Entry: [
                { Scope: [{
                    $: { type: 'GroupById' },
                    ID: [projectGroupId],
                    Name: [projectGroupName] },
                ], Permission: ['FULL_CONTROL'] },
                { Scope: [{
                    $: { type: 'AllUsers' } },
                ],
                    Permission: ['WRITE'] },
            ] },
        ],
    },
};

function parseRespAndAssert(xml, callback) {
    return xml2js.parseString(xml, (err, res) => {
        if (err) {
            process.stdout.write(`err in parsing response ${err}\n`);
            return callback(err);
        }
        assert.deepStrictEqual(res, aclExpectedObject);
        return callback();
    });
}

describe('GCP: PUT Bucket ACL ', function testSuite() {
    this.timeout(30000);
    let config;
    let gcpClient;

    before(() => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
    });

    beforeEach(function beforeFn(done) {
        this.currentTest.bucketName = `somebucket-${Date.now()}`;
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

    describe('when user does not have ACL permissions', () => {
        let gcpClient2;

        before(() => {
            const config2 = getRealAwsConfig(credentialTwo);
            gcpClient2 = new GCP(config2);
        });

        it('should return 403 and AccessDenied', function testFn(done) {
            gcpClient2.putBucketAcl({
                Bucket: this.test.bucketName,
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
                next => gcpClient.putBucketAcl({
                    Bucket: this.test.bucketName,
                    ACL: 'public-read-write',
                    AccessControlPolicy: {},
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got err ${err}`);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'GET',
                    bucket: this.test.bucketName,
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
                next => gcpClient.putBucketAcl({
                    Bucket: this.test.bucketName,
                    AccessControlPolicy: {
                        Grants: [
                            {
                                Grantee: {
                                    Type: 'GroupById',
                                    ID: projectGroupId,
                                },
                                Permission: 'FULL_CONTROL',
                            },
                            {
                                Grantee: {
                                    Type: 'AllUsers',
                                },
                                Permission: 'WRITE',
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
                    bucket: this.test.bucketName,
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
                next => gcpClient.putBucketAcl({
                    Bucket: this.test.bucketName,
                    GrantFullControl: `GroupById=${projectGroupId}`,
                    GrantWrite: 'AllUsers',
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got err ${err}`);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'GET',
                    bucket: this.test.bucketName,
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
