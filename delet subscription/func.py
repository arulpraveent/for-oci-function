import io
import os
import json
import sys
import logging
from fdk import response
import pandas
from urllib.parse import urlparse, parse_qs

import oci.object_storage

def handler(ctx, data: io.BytesIO=None):
    try:
        requesturl = ctx.RequestURL()
        logging.getLogger().info("Request URL: " + json.dumps(requesturl))
    
        # retrieving query string from the request URL, e.g. {"param1":["value"]}
        parsed_url = urlparse(requesturl)
        body = parse_qs(parsed_url.query)
        bucketName = "Bucket-for-crop-health-project"
        objectName = "check_health_file_obj.csv"
        logging.getLogger().info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        logging.getLogger().info("Query string: " + json.dumps(body))
        mail = body["mail"][0].lower()
        farm_name = body["farm"][0]
        if farm_name == "all":
            a = get_object(bucketName,objectName)
            b = io.StringIO(a.decode(encoding='UTF-8'))
            df = pandas.read_csv(b,index_col=0)
            index_names = df[ (df['mail id'] == mail)].index
            df.drop(index_names, inplace = True)
            df.reset_index(inplace=True,drop=True)  
            wf = df.to_csv()
        else:
            a = get_object(bucketName,objectName)
            b = io.StringIO(a.decode(encoding='UTF-8'))
            df = pandas.read_csv(b,index_col=0)
            index_names = df[ (df['mail id'] == mail) & (df['farm_name'] == farm_name)].index
            df.drop(index_names, inplace = True)
            df.reset_index(inplace=True,drop=True)  
            wf = df.to_csv()
        resp = put_object(bucketName, objectName, wf)
    except Exception as e:
        resp = "Failed: " + str(e.message)
    logging.getLogger().info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    logging.getLogger().info(json.dumps(resp))
    return response.Response(
        ctx,
        response_data=json.dumps(resp),
        headers={"Content-Type": "application/json"}
    )

def put_object(bucketName, objectName, content):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    output=""
    try:
        object = client.put_object(namespace, bucketName, objectName, content)
        output = "The unsubscribe is successfull"
    except Exception as e:
        output = "Failed: " + str(e.message)
    return { "state": output }

def get_object(bucketName,objectName):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    object = client.get_object(namespace, bucketName, objectName)
    return (object.data.content)
