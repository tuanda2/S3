Google Cloud Storage Client
===========================

Overview
--------

This document will cover the GCP Client implementation in the CloudServer repository
and describe the design of S3 methods not implemented in the Google Cloud Storage
XML API. Such methods include the multipart uploads, multiple object deletes, and
object-level tagging.

Due to some of the GCP XML API's limitations and behaviors that differ from AWS,
usage of the GCP JSON API is also required. Specifically, the following JSON
requests: object rewrite, object update, and batch operations.

Reference:
`JSON Object APIs <https://cloud.google.com/storage/docs/json_api>`__

Setup
-----
Refer to the `USING PUBLIC CLOUDS Documentation <../USING_PUBLIC_CLOUDS/#google-cloud-storage-as-a-data-backend>`__
to setup Google Cloud Storage as a data backend.

GCP Client
----------

The GCP Client is implemented using the Amazon S3 javascript library,
:code:`aws-sdk`.

Inheriting from the :code:`S3 Service Class`, the native Google Cloud API methods
can be described in a JSON file to be loaded and parsed with :code:`aws-sdk`. This
allows for Google Cloud Storage requests to be similar to Amazon S3 request
in request/response headers and reduces code.

API Template:

.. code::

    "version": "1.0",
    "metadata": {
        "apiVersion": "2017-11-01",
        "checksumFormat": "md5",
        "endpointPrefix": "s3",
        "globalEndpoint": "storage.googleapi.com",
        "protocol": "rest-xml",
        "serviceAbbreviation": "GCP",
        "serviceFullName": "Google Cloud Storage",
        "signatureVersion": "s3",
        "timestampFormat": "rfc822",
        "uid": "gcp-2017-11-01"
    },
    "operations": {
    (...)
        "Method Name": {
            "http": {
                "method": "", // PUT|GET|HEAD|DELETE|POST
                "requestUri": "", // path and query parameters to be appended
            },
            "input": {
                "type": "", // structure|list|map
                "required": [], // if any required input(s)

                // if type == structure
                "members": {
                    "Name": {
                        "location": "" // header|uri|querystring
                        "locationName": "" // mapping Name -> locationName in request
                        "xmlAttribute": boolean // if xml attribute
                    },
                    (...)
                }

                // if type == list
                "member": {
                    "Name": {
                        "location": "", // header|uri|querystring
                        "locationName": "", // mapping Name -> locationName in request
                        "xmlAttribute": boolean // if xml attribute
                    },
                    (...)
                },
                "flattend": boolean, // if list items are flattend

                // if type == map
                "key": {},
                "value": {}
            },
            "output": {
                // items are similar to input template aside from the behavior of locationName
                // in output, the mapping is done from the response locationName -> Name
                (...)
            }
        },
    (...)
    },
    "shapes": {
        "Shape name": {
            // description of shape
        }
    }

GCP Multipart Upload
--------------------
GCP does not have the methods for AWS S3 style of multipart upload. To support such
feature and due to limitations of the compose operation, extra buckets are needed
for a GCP backend: the mpu bucket and the overflow bucket. These bucket serve to
hide the background operations and to 'reset' the composite count of GCP objects.
One thing to note, mpu bucket must be of a different storage class from the main bucket
and overflow bucket.

The main bucket and overflow bucket will be set to :code:`multi_regional` and :code:`USA` region
The mpu bucket will be set to :code:`regional` and whichever regional zone is best.

To keep track of parts and intermediate parts when performing merges, objects uploaded
are of the form `{object}-{uploadid}-{phase}/{number}`.

| Reference:
| `Compose Limitation <https://cloud.google.com/storage/docs/composite-objects>`__
| `COPY Documentation <https://cloud.google.com/storage/docs/json_api/v1/objects/copy>`__
| Note the paragraph that specifies the use of rewrite for copying large objects
| `Rewrite Documentation <https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite>`__
| Note on rewrite, rewriting objects from buckets of different storage class or region
  will reset an object's component count

Initiate Multipart Upload
~~~~~~~~~~~~~~~~~~~~~~~~~
A temporary object is created in the MPU bucket to store the headers/metadata
intended for the final object, then a generated upload id is returned.
The upload id will be used to prefix part uploads and performing other MPU operations.

Upload Part and Upload Part Copy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
These operations are similar to the regular Object PUT and COPY with the addition
of adding the object key prefix.

On the CloudServer, because GCP won't be keeping track of parts, parts will be kept
track of in the metadata shadowbucket.

Abort Multipart Upload
~~~~~~~~~~~~~~~~~~~~~~
All versions of the GCP multipart upload objects are retrieved via the :code:`listObjectVersions`
method with the querystring prefix set to the mpu-prefix and deleted with the JSON batch
operation.

Complete Multipart Upload
~~~~~~~~~~~~~~~~~~~~~~~~~
Complete multipart upload for GCP will require multiple phases: first compose,
second compose, object rewrite, final compose, object copy, and part deletion.

| Phases:
| First Compose - perform the first round of compose
| Ex. Max 10000 Objects -> 313 Objects (component count of 32 per object)
| Second Compose - perform the second round of compose
| Ex. 313 Objects -> 10 (component count of 1024 per object)
| Rewrite - as the objects have reach the max component count, a rewrite is required
  to 'reset' the count. (from mpu bucket to overflow bucket)
| Final compose - perform the final merge to complete the mpu object
| Copy - retrieve the headers from the init object and copy with directive REPLACE
  from overflow to main bucket.
| Delete parts - delete all the objects versions used in the complete mpu operation
|

| With compose, etag is not generated. One will need to be generated at the end of
  the operation.
| Resources about the subject:
| `<https://forums.aws.amazon.com/thread.jspa?messageID=203510#203510>`__
| `<http://permalink.gmane.org/gmane.comp.file-systems.s3.s3tools/583>`__

List MPU Parts
~~~~~~~~~~~~~~
Listing MPU parts can be done with a Bucket GET with querystring :code:`prefix`.

However, for CloudServer, parts can be obtained with call to metadata for the shadowbucket.

GCP Object Level Tagging
------------------------
To support object level tagging, tags will be stored as metadata prefixed with :code:`aws-tag-`

GCP Versioning with CloudServer
-------------------------------
| GCP does not support the same method of versioning as AWS

`<https://cloud.google.com/storage/docs/object-versioning>`__
