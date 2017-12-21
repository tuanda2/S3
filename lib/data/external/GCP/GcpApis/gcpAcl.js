const errors = require('arsenal');
const { grantProps, cannedAclGcp, gcpGrantTypes, awsGrantMapping, awsAcpMapping,
    permissionsAwsToGcp } = require('../GcpUtils');
const GcpRequest = require('../GcpRequest');

function _hasProperty(params) {
    const gotProps = [];
    Object.keys(grantProps).forEach(property => {
        if (property in params) {
            gotProps.push(property);
        }
    });
    return gotProps;
}

function _getKeyFormat(key) {
    const awsKeyMap = awsGrantMapping[key];
    if (awsKeyMap) {
        return { type: awsKeyMap, field: gcpGrantTypes[awsKeyMap] };
    }
    return { type: key, field: gcpGrantTypes[key] };
}

function _parseGrantHeaders(grantParams, params) {
    const accessControlPolicy = [];
    grantParams.forEach(grantName => {
        const itemList = params[grantName];
        const itemArray = itemList.split(',').map(item => item.trim());
        itemArray.forEach(item => {
            const arr = item.split('=');
            const key = _getKeyFormat(arr[0]);
            const value = arr.length > 1 && arr[1] || '';
            const granteeObject = {
                Grantee: {
                    Type: key.type,
                },
                Permission: grantProps[grantName],
            };
            if (key.field && key.field === 'emailAddress' && value) {
                granteeObject.Grantee.EmailAddress = value;
            } else if (key.field && key.field === 'domain' && value) {
                granteeObject.Grantee.Domain = value;
            } else if (key.field && key.field === 'id' && value) {
                granteeObject.Grantee.ID = value;
            }
            accessControlPolicy.push(granteeObject);
        });
    });
    return accessControlPolicy;
}

function _mapUserAcp(userAcp) {
    const mappedAcp = userAcp;
    mappedAcp.Grants = userAcp.Grants.map(grantee => {
        const retGrantee = grantee;
        retGrantee.Grantee.Type =
            awsAcpMapping[grantee.Grantee.Type] || grantee.Grantee.Type;
        retGrantee.Permission = permissionsAwsToGcp[grantee.Permission];
        return retGrantee;
    });
    return mappedAcp;
}

function _aclPermissions(params, callback) {
    const grantParams = _hasProperty(params);
    if (grantParams.length) {
        return {
            AccessControlPolicy: {
                Grants: _parseGrantHeaders(grantParams, params),
            },
        };
    }
    if (params.ACL) {
        if (cannedAclGcp[params.ACL]) {
            return { ACL: params.ACL };
        }
        callback(errors.NotImplemented.customizeDescription(
            `${params.ACL} canned ACL not implemented`));
    }
    if (params.AccessControlPolicy) {
        return {
            AccessControlPolicy: _mapUserAcp(params.AccessControlPolicy),
        };
    }
    return {};
}

function _putBucketAcl(params, callback) {
    const mappedParams = {
        Bucket: params.Bucket,
        ProjectId: params.ProjectId,
    };
    Object.assign(mappedParams, _aclPermissions(params, callback));
    return this.putBucketAclReq(mappedParams, callback);
}

function _putObjectAcl(params, callback) {
    const mappedParams = {
        Bucket: params.Bucket,
        Key: params.Key,
        VersionId: params.VersionId,
        ContentMD5: params.ContentMD5,
        ProjectId: params.ProjectId,
    };
    Object.assign(mappedParams, _aclPermissions(params, callback));
    return this.putObjectAclReq(mappedParams, callback);
}

module.exports = {
    putBucketAcl(params, callback) {
        const req = new GcpRequest(this, _putBucketAcl, params);
        if (callback && typeof callback === 'function') {
            req.send(callback);
        }
        return req;
    },
    putObjectAcl(params, callback) {
        const req = new GcpRequest(this, _putObjectAcl, params);
        if (callback && typeof callback === 'function') {
            req.send(callback);
        }
        return req;
    },
};
