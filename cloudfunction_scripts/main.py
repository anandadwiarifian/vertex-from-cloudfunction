import base64
import json
import logging
from createDataprocJob import createDataprocJob
from executeNotebookCustomModel import executeNotebookCustomModel

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        pubsub_message = json.loads(base64.b64decode(event['data']))
        
        MODEL_TYPE = pubsub_message["model_type"]
        if MODEL_TYPE == "custom_model":
            TEMPLATE_URI = pubsub_message["template_uri"]
            PROJECT_ID = pubsub_message["project_id"]
            TARGET_TABLE = pubsub_message["target_table"]
            MODEL_URI = pubsub_message["model_uri"]
            SCALER_URI = pubsub_message.get("scaler_uri", None)

            executeNotebookCustomModel(
                template_uri=TEMPLATE_URI, project_id=PROJECT_ID,
                target_table=TARGET_TABLE, model_uri=MODEL_URI,
                scaler_uri=SCALER_URI
            )
        elif MODEL_TYPE == "generic_model_pyspark":
            PROJECT_ID = pubsub_message["project_id"]
            PYSPARK_SCRIPT = pubsub_message["pyspark_script"]
            CUSTOM_IMAGE_TAG = pubsub_message["custom_image_tag"]
            PARAMETERS = pubsub_message["parameters"]

            createDataprocJob(
                project_id=PROJECT_ID, pyspark_script=PYSPARK_SCRIPT, 
                custom_image_tag=CUSTOM_IMAGE_TAG, parameters=PARAMETERS
            )
        else:
            raise Exception("model_type is not recognized")
                       
    except Exception as e:
        try:
            pubsub_message_error = str(pubsub_message)
        except:
            pubsub_message_error = event['data']
        logging.exception('Error. message content: '+pubsub_message_error)

