const werelogs = require('werelogs');

const _config = require('../../../Config.js').config;

werelogs.configure({
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});

const logger = new werelogs.Logger('gcpMPU');

const grantProps = {
    GrantFullControl: 'FULL_CONTROL',
    GrantRead: 'READ',
    GrantReadACP: 'FULL_CONTROL',
    GrantWrite: 'WRITE',
    GrantWriteACP: 'FULL_CONTROL',
};

const cannedAclGcp = {
    'aws-exec-read': false,
    'log-delivery-write': false,
    'private': true,
    'bucket-owner-read': true,
    'bucket-owner-full-control': true,
    'public-read': true,
    'public-read-write': true,
    'authenticated-read': true,
    'project-private': true,
};

const permissionsAwsToGcp = {
    FULL_CONTROL: 'FULL_CONTROL',
    WRITE: 'WRITE',
    READ: 'READ',
    WRITE_ACP: 'FULL_CONTROL',
    READ_ACP: 'FULL_CONTROL',
};

const gcpGrantTypes = {
    UserByEmail: 'emailAddress',
    UserById: 'id',
    GroupByEmail: 'emailAddress',
    GroupById: 'id',
    GroupByDomain: 'domain',
    AllAuthenticatedUsers: true,
    AllUsers: true,
};

const awsGrantMapping = {
    emailAddress: 'UserByEmail',
    id: 'UserById',
    uri: false,
};

const awsAcpMapping = {
    CanonicalUser: 'UserById',
    AmazonCustomerByEmail: 'UserByEmail',
    Group: false,
};

function eachSlice(size) {
    this.array = [];
    let partNumber = 1;
    for (let ind = 0; ind < this.length; ind += size) {
        this.array.push({
            Parts: this.slice(ind, ind + size),
            PartNumber: partNumber++,
        });
    }
    return this.array;
}

function getRandomInt(min, max) {
    /* eslint-disable no-param-reassign */
    min = Math.ceil(min);
    max = Math.floor(max);
    /* eslint-enable no-param-reassign */
    return Math.floor(Math.random() * (max - min)) + min;
}

function createMpuKey(key, uploadId, partNumber, fileName) {
    /* eslint-disable no-param-reassign */
    if (typeof partNumber === 'string' && fileName === undefined) {
        fileName = partNumber;
        partNumber = null;
    }
    /* esline-enable no-param-reassign */
    if (fileName && typeof fileName === 'string') {
        // if partNumber is given, return a "full file path"
        // else return a "directory path"
        return partNumber ? `${key}-${uploadId}/${fileName}/${partNumber}` :
            `${key}-${uploadId}/${fileName}`;
    }
    if (partNumber && typeof partNumber === 'number') {
        // filename wasn't passed as an argument. Create default
        return `${key}-${uploadId}/parts/${partNumber}`;
    }
    // returns a "directory parth"
    return `${key}-${uploadId}/`;
}

function createMpuList(params, level, size) {
    // populate and return a parts list for compose
    const retList = [];
    for (let i = 1; i <= size; ++i) {
        retList.push({
            PartName: `${params.Key}-${params.UploadId}/${level}/${i}`,
            PartNumber: i,
        });
    }
    return retList;
}

module.exports = {
    // structs
    grantProps,
    cannedAclGcp,
    permissionsAwsToGcp,
    gcpGrantTypes,
    awsGrantMapping,
    awsAcpMapping,
    // functions
    eachSlice,
    getRandomInt,
    createMpuKey,
    createMpuList,
    // objects
    logger,
};
