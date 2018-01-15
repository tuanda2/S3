const assert = require('assert');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultiple,
    memLocation,
    fileLocation,
    awsLocation,
    awsLocationMismatch,
    gcpLocation,
    gcpLocationMismatch,
} = require('../utils');

const bucket = 'buckettestmultiplebackenddelete';
const memObject = `memObject-${Date.now()}`;
const fileObject = `fileObject-${Date.now()}`;
const awsObject = `awsObject-${Date.now()}`;
const awsEmptyObject = `awsEmptyObject-${Date.now()}`;
const awsBigObject = `awsBigObject-${Date.now()}`;
const awsMismatchObject = `awsMismatchOjbect-${Date.now()}`;
const gcpObject = `gcpObject-${Date.now()}`;
const gcpEmptyObject = `gcpEmptyObject-${Date.now()}`;
const gcpBigObject = `gcpBigObject-${Date.now()}`;
const gcpMismatchObject = `gcpMismatchOjbect-${Date.now()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);

describeSkipIfNotMultiple('Multiple backend delete', function testSuite() {
    this.timeout(80000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            process.stdout.write('Creating bucket\n');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            })
            .then(() => {
                process.stdout.write('Putting object to mem\n');
                const params = { Bucket: bucket, Key: memObject, Body: body,
                    Metadata: { 'scal-location-constraint': memLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting object to file\n');
                const params = { Bucket: bucket, Key: fileObject, Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting object to AWS\n');
                const params = { Bucket: bucket, Key: awsObject, Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting 0-byte object to AWS\n');
                const params = { Bucket: bucket, Key: awsEmptyObject,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting large object to AWS\n');
                const params = { Bucket: bucket, Key: awsBigObject,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting object to AWS\n');
                const params = { Bucket: bucket, Key: awsMismatchObject,
                    Body: body, Metadata:
                    { 'scal-location-constraint': awsLocationMismatch } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting object to GCP\n');
                const params = { Bucket: bucket, Key: gcpObject, Body: body,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting 0-byte object to GCP\n');
                const params = { Bucket: bucket, Key: gcpEmptyObject,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting large object to GCP\n');
                const params = { Bucket: bucket, Key: gcpBigObject,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting object to GCP\n');
                const params = { Bucket: bucket, Key: gcpMismatchObject,
                    Body: body, Metadata:
                    { 'scal-location-constraint': gcpLocationMismatch } };
                return s3.putObjectAsync(params);
            })
            .catch(err => {
                process.stdout.write(`Error putting objects: ${err}\n`);
                throw err;
            });
        });
        after(() => {
            process.stdout.write('Deleting bucket\n');
            return bucketUtil.deleteOne(bucket)
            .catch(err => {
                process.stdout.write(`Error deleting bucket: ${err}\n`);
                throw err;
            });
        });

        it('should delete object from mem', done => {
            s3.deleteObject({ Bucket: bucket, Key: memObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: memObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete object from file', done => {
            s3.deleteObject({ Bucket: bucket, Key: fileObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: fileObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete object from AWS', done => {
            s3.deleteObject({ Bucket: bucket, Key: awsObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: awsObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete 0-byte object from AWS', done => {
            s3.deleteObject({ Bucket: bucket, Key: awsEmptyObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: awsEmptyObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete large object from AWS', done => {
            s3.deleteObject({ Bucket: bucket, Key: awsBigObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: awsBigObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete object from AWS location with bucketMatch set to ' +
        'false', done => {
            s3.deleteObject({ Bucket: bucket, Key: awsMismatchObject },
            err => {
                assert.equal(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: awsMismatchObject },
                err => {
                    assert.strictEqual(err.code, 'NoSuchKey',
                        'Expected error but got success');
                    done();
                });
            });
        });
        it('should delete object from GCP', done => {
            s3.deleteObject({ Bucket: bucket, Key: gcpObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: gcpObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete 0-byte object from GCP', done => {
            s3.deleteObject({ Bucket: bucket, Key: gcpEmptyObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: gcpEmptyObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete large object from GCP', done => {
            s3.deleteObject({ Bucket: bucket, Key: gcpBigObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: gcpBigObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete object from AWS location with bucketMatch set to ' +
        'false', done => {
            s3.deleteObject({ Bucket: bucket, Key: gcpMismatchObject },
            err => {
                assert.equal(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: gcpMismatchObject },
                err => {
                    assert.strictEqual(err.code, 'NoSuchKey',
                        'Expected error but got success');
                    done();
                });
            });
        });
    });
});
