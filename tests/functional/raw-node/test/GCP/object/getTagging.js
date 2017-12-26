const assert = require('assert');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${Date.now()}`;
const gcpTagPrefix = 'x-goog-meta-aws-tag-';
const tagSize = 10;
const expectedTagObj = [];

function createTags(size) {
    const retObj = {};
    for (let i = 1; i <= size; ++i) {
        retObj[`${gcpTagPrefix}testtag${i}`] = `testtag${i}`;
        expectedTagObj.push({
            Key: `testtag${i}`,
            Value: `testtag${i}`,
        });
    }
    return retObj;
}

describe('GCP: GET Object Tagging', () => {
    let config;
    let gcpClient;

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        gcpRequestRetry({
            method: 'PUT',
            bucket: bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err}`);
            }
            return done(err);
        });
    });

    beforeEach(function beforeFn(done) {
        this.currentTest.key = `somekey-${Date.now()}`;
        this.currentTest.specialKey = `veryspecial-${Date.now()}`;
        makeGcpRequest({
            method: 'PUT',
            bucket: bucketName,
            objectKey: this.currentTest.key,
            authCredentials: config.credentials,
            headers: createTags(tagSize),
        }, (err, res) => {
            if (err) {
                process.stdout.write(`err in creating object ${err}`);
                return done(err);
            }
            this.currentTest.versionId = res.headers['x-goog-generation'];
            return done();
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
                process.stdout.write(`err in deleting object ${err}`);
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
                process.stdout.write(`err in deleting bucket ${err}`);
            }
            return done(err);
        });
    });

    it('should successfully get object tags', function testFn(done) {
        gcpClient.getObjectTagging({
            Bucket: bucketName,
            Key: this.test.key,
            VersionId: this.test.versionId,
        }, (err, res) => {
            assert.equal(err, null,
                `Expected success, got error ${err}`);
            assert.deepStrictEqual(res.TagSet, expectedTagObj);
            return done();
        });
    });
});
