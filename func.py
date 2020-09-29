import io
import os
import json
import sys
import logging
from fdk import response
import pandas

import oci.object_storage

def handler(ctx, data: io.BytesIO=None):
    try:
        requestbody_str = data.getvalue().decode('UTF-8')
        body = json.loads(requestbody_str)
        bucketName = "Bucket-for-crop-health-project"
        objectName = "check_health_file_obj.csv"
        loc = body["loc"]
        mail = body["mail"]
        a = get_object(bucketName,objectName)
        b = io.StringIO(a.decode(encoding='UTF-8'))
        df1 = pandas.read_csv(b)
        df = pandas.DataFrame()
        df['mail id'] = mail
        df['location'] = loc
        df2 = df.append(df1)
        df2.reset_index(drop=True, inplace=True)
        wf = df2.to_csv()
    except Exception:
        error = """
                Input a JSON object in the format: '{"bucketName": "<bucket name>",
                "content": "<content>", "objectName": "<object name>"}'
                """
        raise Exception(error)
    resp = put_object(bucketName, objectName, wf)
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
        output = "Success: Put object '" + objectName + "' in bucket '" + bucketName + "'"
    except Exception as e:
        output = "Failed: " + str(e.message)
    return { "state": output }

def get_object(bucketName,objectName):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    object = client.get_object(namespace, bucketName, objectName)
    return (object.data.content)
