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
        logging.getLogger().info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        logging.getLogger().info(json.dumps(body))
        app_context = dict(ctx.Config())
        acc_tkn = app_context["acc_tkn"]
        rply = "Access token not valid"
        if acc_tkn != body["acc_tkn"]:
            return response.Response(
            ctx,
            response_data=json.dumps(rply),
            headers={"Content-Type": "application/json"}
            )
        mail = body["mail"].lower()
        farm_name = body["farm"]
        a = get_object(bucketName,objectName)
        b = io.StringIO(a.decode(encoding='UTF-8'))
        df = pandas.read_csv(b,index_col=0)
        index_names = df[ (df['mail id'] == mail) & (df['farm_name'] == farm_name)].index
        df.drop(index_names, inplace = True)
        df.reset_index(inplace=True,drop=True)  
        wf = df.to_csv()
    except Exception:
        error = """
                Input a JSON object in the format: '{"bucketName": "<bucket name>",
                "content": "<content>", "objectName": "<object name>"}'
                """
        raise Exception(error)
    resp = put_object(bucketName, objectName, wf)
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
