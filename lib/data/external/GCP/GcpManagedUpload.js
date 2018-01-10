const AWS = require('aws-sdk');
const { byteLength } = AWS.util.string;
const async = require('async');
const stream = require('stream');
const { errors } = require('arsenal');
const { minimumAllowedPartSize, maximumAllowedPartCount } =
    require('../../../../constants');
const { createMpuList } = require('./GcpUtils');

function sliceFn(size) {
    this.array = [];
    let partNumber = 1;
    for (let ind = 0; ind < this.length; ind += size) {
        this.array.push({
            Body: this.slice(ind, ind + size),
            PartNumber: partNumber++,
        });
    }
    return this.array;
}

class GcpManagedUpload {
    constructor(service, params, options = {}) {
        this.service = service;
        this.params = params;
        this.mainBucket =
            this.params.Bucket || this.service.config.mainBucket;
        this.mpuBucket = this.params.MPU || this.service.config.mpuBucket;
        this.overflowBucket =
            this.params.Overflow || this.service.config.overflowBucket;

        this.partSize = minimumAllowedPartSize;
        this.queueSize = options.queueSize || 4;
        this.validateBody();
        this.configureBytes();

        // multipart information
        this.parts = {};
        this.totalParts = this.totalBytes ?
            Math.ceil(this.totalBytes / this.partSize) : undefined;
        this.uploadedParts = 0;
        this.activeParts = 0;
        this.partBuffers = [];
        this.partQueue = [];
        this.partBufferLength = 0;
        this.totalChunkedBytes = 0;
        this.partNumber = 0;
    }

    validateBody() {
        this.body = this.params.Body;
        if (!this.body) {
            throw errors.InvalidArgument.customizeDescription(
                'Body parameter is required');
        }
        if (typeof this.body === 'string') {
            this.body = Buffer.from(this.body);
        }
        if (this.body instanceof Buffer) {
            this.slicedParts = sliceFn.call(this.body, this.partSize);
        }
    }

    configureBytes() {
        try {
            this.totalBytes = byteLength(this.body);
        } catch (e) {
            this.totalBytes = this.params.ContentLength || 0;
        }
        if (this.totalBytes) {
            const newPartSize =
                Math.ceil(this.totalBytes / maximumAllowedPartCount);
            if (newPartSize > this.partSize) this.partSize = newPartSize;
        }
    }

    abort() {
        // user called abort
        this.cleanUp(new Error('User called abort'));
    }

    cleanUp(err) {
        // is only called when an error happens
        if (this.failed) return;

        this.failed = true;
        // clean variables
        this.activeParts = 0;
        this.partBuffers = [];
        this.partBuffertLength = 0;
        this.partNumber = 0;

        if (this.uploadId) {
            // if MPU was successfuly created
            this.abortMPU(mpuErr => {
                if (mpuErr) { // double error
                    this.callback(errors.InternalError
                        .customizeDescription(
                            'Unable to abort MPU after upload failure'));
                }
                this.callback(err);
            });
        } else {
            this.callback(err);
        }
    }

    abortMPU(callback) {
        const params = {
            Bucket: this.mainBucket,
            MPU: this.mpuBucket,
            Overflow: this.overflowBucket,
            UploadId: this.uploadId,
            Key: this.params.Key,
        };
        this.service.abortMultipartUpload(params, err => callback(err));
    }

    completeUpload(callback) {
        const params = {
            Bucket: this.mainBucket,
            MPU: this.mpuBucket,
            Overflow: this.overflowBucket,
            Key: this.params.Key,
            UploadId: this.uploadId,
            MultipartUpload: {
                Parts: [],
            },
        };
        params.MultipartUpload.Parts =
            createMpuList(params, 'parts', this.uploadedParts);
        this.service.completeMultipartUpload(params,
        (err, res) => {
            if (callback && typeof callback === 'function') {
                callback(err, res);
            } else {
                if (err) {
                    this.cleanUp(err);
                } else {
                    this.callback(null, res);
                }
            }
        });
    }

    send(callback) {
        this.failed = false;
        if (this.callback) return;
        this.callback = callback || function newCallback(err) {
            if (err) throw err;
        };
        if (this.totalBytes <= this.partSize) {
            this.uploadSingle();
        } else if (this.slicedParts) {
            this.uploadBufferSlices();
        } else if (this.body instanceof stream) {
            // stream type
            this.body.on('error', err => this.cleanUp(err))
            .on('readable', () => this.chunkStream())
            .on('end', () => {
                this.isDoneChunking = true;
                this.chunkStream();

                if (this.isDoneChunking && this.uploadedParts >= 1 &&
                    this.uploadedParts === this.totalParts) {
                    this.completeUpload();
                }
            });
        }
    }

    uploadSingle() {
        // use putObject to upload the single part object
        const params = this.params;
        params.Bucket = this.mainBucket;
        this.service.putObject(params, (err, res) => {
            if (err) {
                this.cleanUp(err);
            } else {
                // return results from a putObject request
                this.callback(null, res);
            }
        });
    }

    uploadBufferSlices() {
        if (this.slicedParts.length <= 1 && this.totalParts) {
            // there is only one part
            this.uploadSingle();
        } else {
            // multiple slices
            async.waterfall([
                // createMultipartUpload
                next => {
                    const params = this.params;
                    params.Bucket = this.mpuBucket;
                    this.service.createMultipartUpload(params, (err, res) => {
                        if (!err) {
                            this.uploadId = res.UploadId;
                        }
                        return next(err);
                    });
                },
                next => async.mapLimit(this.slicedParts, this.queueSize,
                (uploadPart, moveOn) => {
                    const params = {
                        Bucket: this.mpuBucket,
                        Key: this.params.Key,
                        UploadId: this.uploadId,
                        Body: uploadPart.Body,
                        PartNumber: uploadPart.PartNumber,
                    };
                    this.service.uploadPart(params, err => {
                        if (!err) {
                            this.uploadedParts++;
                        }
                        moveOn(err);
                    });
                }, err => next(err)),
                next => this.completeUpload(next),
            ], (err, results) => {
                if (err) {
                    this.cleanup(err);
                } else {
                    this.callback(null, results);
                }
            });
        }
    }

    chunkStream() {
        if (this.activeParts > this.queueSize) return;

        const buf = this.body.read(this.partSize - this.partBufferLength) ||
            this.body.read();

        if (buf) {
            this.partBuffers.push(buf);
            this.partBufferLength += buf.length;
            this.totalChunkedBytes += buf.length;
        }

        let pbuf;
        if (this.partBufferLength >= this.partSize) {
            pbuf = this.partBuffers.length === 1 ?
                this.partBuffers[0] : Buffer.concat(this.partBuffers);
            this.partBuffers = [];
            this.partBufferLength = 0;

            if (pbuf.length > this.partSize) {
                const rest = pbuf.slice(this.partSize);
                this.partBuffers.push(rest);
                this.partBufferLength += rest.length;
                pbuf = pbuf.slice(0, this.partSize);
            }
            this.processChunk(pbuf);
        }

        // when chunking the last part
        if (this.isDoneChunking && !this.completed) {
            this.completed = true;
            pbuf = this.partBuffers.length === 1 ?
                this.partBuffers[0] : Buffer.concat(this.partBuffers);
            this.partBuffers = [];
            this.partBufferLength = 0;
            if (this.uploadedParts <= 0 || pbuf.length > 0) {
                this.processChunk(pbuf);
            }
        }

        this.body.read(0);
    }

    processChunk(chunk) {
        const partNumber = ++this.partNumber;
        if (!this.uploadId) {
            // if multipart upload does not exist
            if (!this.multipartReq) {
                const params = this.params;
                params.Bucket = this.mpuBucket;
                this.multipartReq =
                    this.service.createMultipartUpload(params, (err, res) => {
                        if (err) {
                            this.cleanUp();
                        } else {
                            this.uploadId = res.UploadId;
                            this.uploadChunk(chunk, partNumber);
                            if (this.partQueue.length > 0) {
                                this.partQueue.forEach(
                                    part => this.uploadChunk(...part));
                            }
                        }
                    });
            } else {
                this.partQueue.push([chunk, partNumber]);
            }
        } else {
            // queues chunks for upload
            this.uploadChunk(chunk, partNumber);
            this.activeParts++;
            if (this.activeParts < this.queueSize) {
                this.chunkStream();
            }
        }
    }

    uploadChunk(chunk, partNumber) {
        const params = {
            Bucket: this.mpuBucket,
            Key: this.params.Key,
            UploadId: this.uploadId,
            PartNumber: partNumber,
            Body: chunk,
            ContentLength: chunk.length,
        };
        this.service.uploadPart(params, err => {
            if (err) {
                this.cleanUp(err);
            } else {
                this.uploadedParts++;
                this.activeParts--;
                if (this.totalParts === this.uploadedParts &&
                    this.isDoneChunking) {
                    this.completeUpload();
                } else {
                    this.chunkStream();
                }
            }
        });
    }
}

module.exports = GcpManagedUpload;
