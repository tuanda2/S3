Google Cloud Storage Client
===========================

Overview
--------

This document will cover the GCP Client implementation in the CloudServer repository
and describe the design of S3 methods not implemented in the Google Cloud Storage
XML API. Such methods include the multipart uploads, multiple object deletes,
object-level tagging, and ACL grant headers.

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


Google Cloud Storage APIs
~~~~~~~~~~~~~~~~~~~~~~~~
.. code::

    - PUT Bucket
        - Set Versioning
    - GET Bucket
        - Get Versioning
    - HEAD Bucket
    - DELETE Bucket

Custom GCP APIs
~~~~~~~~~~~~~~~

.. code::

    To be implemented:
    Delete Objects
    AWS ACL-Grant Headers
    Multipart Upload
    Object Tagging
    Versioning
