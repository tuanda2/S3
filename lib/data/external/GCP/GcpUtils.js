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

module.exports = {
    grantProps,
    cannedAclGcp,
    permissionsAwsToGcp,
    gcpGrantTypes,
    awsGrantMapping,
    awsAcpMapping,
};
