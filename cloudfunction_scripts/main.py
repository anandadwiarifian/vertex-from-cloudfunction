import base64
import json
import logging
import os
from executeNotebookCustomModel import executeNotebookCustomModel
from executeNotebookHeuristicPython import executeNotebookHeuristicPython
# from datetime import date, timedelta
# from datetime import datetime as dt
# from dateutil.relativedelta import relativedelta

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        PROJECT_ID = os.environ.get('GCP_PROJECT')
        TARGET_BUCKET = os.environ.get('TARGET_BUCKET')
        DATAFOUNDATION_TABLE = os.environ.get('DATAFOUNDATION_TABLE')
        BUCKET_PIPELINE = os.environ.get('BUCKET_PIPELINE')
        TOPIC_ID = os.environ.get('TOPIC_ID')

        pubsub_message = json.loads(base64.b64decode(event['data']))
        ORGANIZATION_ID = pubsub_message["organization_id"]

        ## query to CloudSQL to get parameters: model uri, script uri, etc
        ##
        ##
        ## 

        AUDIENCE_ID = 1
        PYTHON_SCRIPT_PATH = os.environ.get('PYTHON_SCRIPT_PATH')
        
        BUCKET_PIPELINE_AUDIENCE = BUCKET_PIPELINE+"/"+ORGANIZATION_ID

        MODEL_TYPE = pubsub_message["model_type"]
        if MODEL_TYPE == "custom_model":
            SCORING_SCRIPT = pubsub_message["scoring_script"]
            MODEL_URI = pubsub_message["model_uri"]
            SCALER_URI = pubsub_message.get("scaler_uri", None)
            executeNotebookCustomModel(
                template_uri=SCORING_SCRIPT, project_id=PROJECT_ID,
                datafoundation_table=DATAFOUNDATION_TABLE, bucket_pipeline=BUCKET_PIPELINE_AUDIENCE,
                target_bucket=TARGET_BUCKET, model_uri=MODEL_URI, 
                scaler_uri=SCALER_URI, organization_id=ORGANIZATION_ID,
                topic_id=TOPIC_ID, audience_id=AUDIENCE_ID,
            )

        elif MODEL_TYPE == "generic_model_python":
            FEATURE_LIST = pubsub_message["feature_list"]
            MSISDN_SAMPLE_PATH = pubsub_message["msisdn_sample_path"]
            MAX_MSISDN_COUNT = pubsub_message["max_msisdn_count"]
            executeNotebookHeuristicPython(
                project_id=PROJECT_ID, python_script_path=PYTHON_SCRIPT_PATH, 
                datafoundation_table=DATAFOUNDATION_TABLE, bucket_pipeline=BUCKET_PIPELINE_AUDIENCE,
                organization_id=ORGANIZATION_ID, feature_list=FEATURE_LIST, msisdn_sample_path=MSISDN_SAMPLE_PATH,
                max_msisdn_count=MAX_MSISDN_COUNT, topic_id=TOPIC_ID, audience_id=AUDIENCE_ID,
                target_bucket=TARGET_BUCKET
            )
          
        else:
            raise Exception("model_type is not recognized")
                       
    except Exception as e:
        try:
            pubsub_message_error = str(pubsub_message)
        except:
            pubsub_message_error = event['data']
        logging.exception('Error. message content: '+pubsub_message_error)

