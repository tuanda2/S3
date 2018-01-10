module.exports = {
    abortMultipartUpload: require('./abortMPU'),
    completeMultipartUpload: require('./completeMPU'),
    createMultipartUpload: require('./createMPU'),
    listParts: require('./listParts'),
    uploadPart: require('./uploadPart'),
    uploadPartCopy: require('./uploadPartCopy'),
};
