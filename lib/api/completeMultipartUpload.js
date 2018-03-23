const async = require('async');
const { parseString } = require('xml2js');
const { errors, versioning, s3middleware } = require('arsenal');

const convertToXml = s3middleware.convertToXml;
const { pushMetric } = require('../utapi/utilities');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const { validateAndFilterMpuParts, generateMpuPartStorageInfo } =
    require('./apiUtils/object/processMpuParts');
const { config } = require('../Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');

const data = require('../data/wrapper');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const constants = require('../../constants');
const { versioningPreprocessing, checkQueryVersionId }
    = require('./apiUtils/object/versioning');
const metadata = require('../metadata/wrapper');
const services = require('../services');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { skipMpuPartProcessing } = require('../data/external/utils');

const logger = require('../utilities/logger');

const versionIdUtils = versioning.VersionID;

let splitter = constants.splitter;
const REPLICATION_ACTION = 'MPU';

/*
   Format of xml request:
   <CompleteMultipartUpload>
     <Part>
       <PartNumber>1</PartNumber>
       <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
     </Part>
     <Part>
       <PartNumber>2</PartNumber>
       <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
     </Part>
     <Part>
       <PartNumber>3</PartNumber>
       <ETag>"acbd18db4cc2f85cedef654fccc4a4d8"</ETag>
     </Part>
   </CompleteMultipartUpload>
   */


  /*
  Format of xml response:
      <?xml version='1.0' encoding='UTF-8'?>
    <CompleteMultipartUploadResult
    xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
      <Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>
      <Bucket>Example-Bucket</Bucket>
      <Key>Example-Object</Key>
      <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
    </CompleteMultipartUploadResult>
   */

/**
 * completeMultipartUpload - Complete a multipart upload
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function completeMultipartUpload(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'completeMultipartUpload' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const hostname = request.parsedHost;
    const uploadId = request.query.uploadId;
    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        uploadId,
        // Note: permissions for completing a multipart upload are the
        // same as putting a part.
        requestType: 'putPart or complete',
        log,
    };
    const xmlParams = {
        bucketName,
        objectKey,
        hostname,
    };

    const queryContainsVersionId = checkQueryVersionId(request.query);
    if (queryContainsVersionId instanceof Error) {
        return callback(queryContainsVersionId);
    }

    function parseXml(xmlToParse, next) {
        return parseString(xmlToParse, (err, result) => {
            if (err || !result || !result.CompleteMultipartUpload
                || !result.CompleteMultipartUpload.Part) {
                return next(errors.MalformedXML);
            }
            const jsonList = result.CompleteMultipartUpload;
            return next(null, jsonList);
        });
    }

    return async.waterfall([
        function validateDestBucket(next) {
            const metadataValParams = {
                objectKey,
                authInfo,
                bucketName,
                // Required permissions for this action
                // at the destinationBucket level are same as objectPut
                requestType: 'objectPut',
            };
            metadataValidateBucketAndObj(metadataValParams, log, next);
        },
        function validateMultipart(destBucket, objMD, next) {
            services.metadataValidateMultipart(metadataValParams,
                (err, mpuBucket) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(null, destBucket, objMD, mpuBucket);
                });
        },
        function parsePartsList(destBucket, objMD, mpuBucket, next) {
            if (request.post) {
                return parseXml(request.post, (err, jsonList) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(err, destBucket, objMD, mpuBucket, jsonList);
                });
            }
            return next(errors.MalformedXML, destBucket);
        },
        function getMPUMetadata(destBucket, objMD, mpuBucket, jsonList, next) {
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }
            // Reconstruct mpuOverviewKey to serve
            // as key to pull metadata originally stored when mpu initiated
            const mpuOverviewKey =
                `overview${splitter}${objectKey}${splitter}${uploadId}`;

            return metadata.getObjectMD(mpuBucket.getName(), mpuOverviewKey,
            {}, log, (err, storedMetadata) => {
                if (err) {
                    return next(err, destBucket);
                }
                const location = storedMetadata.controllingLocationConstraint;
                return next(null, destBucket, objMD, mpuBucket, jsonList,
                storedMetadata, location, mpuOverviewKey);
            });
        },
        function retrieveParts(destBucket, objMD, mpuBucket, jsonList,
        storedMetadata, location, mpuOverviewKey, next) {
            return services.getMPUparts(mpuBucket.getName(), uploadId, log,
                (err, result) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    const storedParts = result.Contents;
                    return next(null, destBucket, objMD, mpuBucket, storedParts,
                        jsonList, storedMetadata, location, mpuOverviewKey);
                });
        },
        function ifMultipleBackend(destBucket, objMD, mpuBucket, storedParts,
        jsonList, storedMetadata, location, mpuOverviewKey, next) {
            if (config.backends.data === 'multiple') {
                const mdInfo = { storedParts, mpuOverviewKey, splitter };
                return multipleBackendGateway.completeMPU(objectKey,
                uploadId, location, jsonList, mdInfo, bucketName, null, null,
                log, (err, completeObjData) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    return next(null, destBucket, objMD, mpuBucket, storedParts,
                        jsonList, storedMetadata, completeObjData,
                        mpuOverviewKey);
                });
            }
            return next(null, destBucket, objMD, mpuBucket, storedParts,
                jsonList, storedMetadata, null, mpuOverviewKey);
        },
        function validateAndFilterParts(destBucket, objMD, mpuBucket,
        storedParts, jsonList, storedMetadata, completeObjData, mpuOverviewKey,
        next) {
            if (completeObjData) {
                return next(null, destBucket, objMD, mpuBucket, storedParts,
                jsonList, storedMetadata, completeObjData, mpuOverviewKey,
                completeObjData.filteredPartsObj);
            }
            const filteredPartsObj = validateAndFilterMpuParts(storedParts,
                jsonList, mpuOverviewKey, splitter, log);
            if (filteredPartsObj.error) {
                return next(filteredPartsObj.error, destBucket);
            }
            return next(null, destBucket, objMD, mpuBucket, storedParts,
                jsonList, storedMetadata, completeObjData, mpuOverviewKey,
                filteredPartsObj);
        },
        function processParts(destBucket, objMD, mpuBucket, storedParts,
        jsonList, storedMetadata, completeObjData, mpuOverviewKey,
        filteredPartsObj, next) {
            // if mpu was completed on backend that stored mpu MD externally,
            // skip MD processing steps
            if (completeObjData && skipMpuPartProcessing(completeObjData)) {
                const dataLocations = [
                    {
                        key: completeObjData.key,
                        size: completeObjData.contentLength,
                        start: 0,
                        dataStoreVersionId: completeObjData.dataStoreVersionId,
                        dataStoreName: storedMetadata.dataStoreName,
                        dataStoreETag: completeObjData.eTag,
                        dataStoreType: completeObjData.dataStoreType,
                    },
                ];
                const calculatedSize = completeObjData.contentLength;
                return next(null, destBucket, objMD, mpuBucket, storedMetadata,
                    completeObjData.eTag, calculatedSize, dataLocations,
                    [mpuOverviewKey], null, completeObjData);
            }

            const partsInfo =
                generateMpuPartStorageInfo(filteredPartsObj.partList);
            if (partsInfo.error) {
                return next(partsInfo.error, destBucket);
            }
            const { keysToDelete, extraPartLocations } = filteredPartsObj;
            const { aggregateETag, dataLocations, calculatedSize } = partsInfo;

            if (completeObjData) {
                const dataLocations = [
                    {
                        key: completeObjData.key,
                        size: calculatedSize,
                        start: 0,
                        dataStoreName: storedMetadata.dataStoreName,
                        dataStoreETag: aggregateETag,
                        dataStoreType: completeObjData.dataStoreType,
                    },
                ];
                return next(null, destBucket, objMD, mpuBucket, storedMetadata,
                    aggregateETag, calculatedSize, dataLocations, keysToDelete,
                    extraPartLocations, completeObjData);
            }
            return next(null, destBucket, objMD, mpuBucket, storedMetadata,
                aggregateETag, calculatedSize, dataLocations, keysToDelete,
                extraPartLocations, null);
        },
        function prepForStoring(destBucket, objMD, mpuBucket, storedMetadata,
            aggregateETag, calculatedSize, dataLocations, keysToDelete,
            extraPartLocations, completeObjData, next) {
            const metaHeaders = {};
            const keysNotNeeded =
                ['initiator', 'partLocations', 'key',
                    'initiated', 'uploadId', 'content-type', 'expires',
                    'eventualStorageBucket', 'dataStoreName'];
            const metadataKeysToPull =
                Object.keys(storedMetadata).filter(item =>
                    keysNotNeeded.indexOf(item) === -1);
            metadataKeysToPull.forEach(item => {
                metaHeaders[item] = storedMetadata[item];
            });
            const location = storedMetadata.controllingLocationConstraint;
            const metaStoreParams = {
                authInfo,
                objectKey,
                metaHeaders,
                uploadId,
                dataStoreName: storedMetadata.dataStoreName,
                contentType: storedMetadata['content-type'],
                cacheControl: storedMetadata['cache-control'],
                contentDisposition: storedMetadata['content-disposition'],
                contentEncoding: storedMetadata['content-encoding'],
                expires: storedMetadata.expires,
                contentMD5: aggregateETag,
                size: calculatedSize,
                multipart: true,
                replicationInfo: getReplicationInfo(objectKey, destBucket,
                    false, calculatedSize, REPLICATION_ACTION),
                log,
            };
            // create non-transient location field as 'PENDING' when
            // the location is transient and CRR is enabled, it will
            // be set by the CRR processor of preferred CRR target to
            // the actual preferred location.
            if(config.locationConstraintIsTransient(location)
               && metaStoreParams.replicationInfo
               && metaStoreParams.replicationInfo.status === 'PENDING') {
                metaStoreParams.nonTransientLocation = 'PENDING';
            }
            const serverSideEncryption =
                      destBucket.getServerSideEncryption();
            let pseudoCipherBundle = null;
            if (serverSideEncryption) {
                pseudoCipherBundle = {
                    algorithm: destBucket.getSseAlgorithm(),
                    masterKeyId: destBucket.getSseMasterKeyId(),
                };
            }
            return versioningPreprocessing(bucketName,
                destBucket, objectKey, objMD, log, (err, options) => {
                    if (err) {
                        // TODO: check AWS error when user requested a specific
                        // version before any versions have been put
                        const logLvl = err === errors.BadRequest ?
                            'debug' : 'error';
                        log[logLvl]('error getting versioning info', {
                            error: err,
                            method: 'versioningPreprocessing',
                        });
                        return next(err, destBucket);
                    }
                    const dataToDelete = options.dataToDelete;
                    metaStoreParams.versionId = options.versionId;
                    metaStoreParams.versioning = options.versioning;
                    metaStoreParams.isNull = options.isNull;
                    metaStoreParams.nullVersionId = options.nullVersionId;
                    return next(null, destBucket, dataLocations,
                        metaStoreParams, mpuBucket, keysToDelete, aggregateETag,
                        objMD, extraPartLocations, pseudoCipherBundle,
                        dataToDelete, completeObjData);
                });
        },
        function storeAsNewObj(destinationBucket, dataLocations,
            metaStoreParams, mpuBucket, keysToDelete, aggregateETag, objMD,
            extraPartLocations, pseudoCipherBundle, dataToDelete,
            completeObjData, next) {
            return services.metadataStoreObject(destinationBucket.getName(),
                dataLocations, pseudoCipherBundle, metaStoreParams,
                (err, res) => {
                    if (err) {
                        return next(err, destinationBucket);
                    }
                    const generatedVersionId = res ? res.versionId : undefined;
                    // in cases where completing mpu overwrites a previous
                    // null version when versioning is suspended or versioning
                    // is not enabled, need to delete pre-existing data
                    // unless the preexisting object and the completed mpu
                    // are on external backends
                    if (dataToDelete) {
                        const newDataStoreName =
                            Array.isArray(dataLocations) && dataLocations[0] ?
                            dataLocations[0].dataStoreName : null;
                        data.batchDelete(dataToDelete, request.method,
                            newDataStoreName,
                            logger.newRequestLoggerFromSerializedUids(log
                            .getSerializedUids()));
                    }
                    return next(null, mpuBucket, keysToDelete, aggregateETag,
                        extraPartLocations, destinationBucket,
                        generatedVersionId);
                });
        },
        function deletePartsMetadata(mpuBucket, keysToDelete, aggregateETag,
            extraPartLocations, destinationBucket, generatedVersionId, next) {
            services.batchDeleteObjectMetadata(mpuBucket.getName(),
                keysToDelete, log, err => next(err, destinationBucket,
                    aggregateETag, generatedVersionId));
            if (extraPartLocations && extraPartLocations.length > 0) {
                data.batchDelete(extraPartLocations, request.method, null,
                    logger.newRequestLoggerFromSerializedUids(log
                    .getSerializedUids()));
            }
        },
    ], (err, destinationBucket, aggregateETag, generatedVersionId) => {
        const resHeaders =
            collectCorsHeaders(request.headers.origin, request.method,
                destinationBucket);
        if (err) {
            return callback(err, null, resHeaders);
        }
        if (generatedVersionId) {
            resHeaders['x-amz-version-id'] =
                versionIdUtils.encode(generatedVersionId);
        }
        xmlParams.eTag = `"${aggregateETag}"`;
        const xml = convertToXml('completeMultipartUpload', xmlParams);
        pushMetric('completeMultipartUpload', log, {
            authInfo,
            bucket: bucketName,
            keys: [objectKey],
        });
        return callback(null, xml, resHeaders);
    });
}

module.exports = completeMultipartUpload;
