const assert = require('assert');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const credentialTwo = 'gcpbackend2';

const aclExpectedObject = {
    Grantee: {
        Type: 'AllUsers',
    },
    Permission: 'READ',
};
const bucketName = `somebucket-${Date.now()}`;

describe('GCP: GET Object ACL ', function testSuite() {
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
            headers: {
                'x-goog-acl': 'public-read',
            },
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
            gcpClient2.getObjectAcl({
                Bucket: bucketName,
                Key: this.test.key,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 403);
                assert.strictEqual(err.code, 'AccessDenied');
                return done();
            });
        });
    });

    describe('when user has ACL permissions', () => {
        it('should retrieve correct ACP', function testFn(done) {
            return gcpClient.getObjectAcl({
                Bucket: bucketName,
                Key: this.test.key,
            }, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got err ${err}`);
                const isTrue = res.Grants.reduce(
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
                return done();
            });
        });
    });
});
