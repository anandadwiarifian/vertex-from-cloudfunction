import base64
import json
import logging

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        pubsub_message = json.loads(base64.b64decode(event['data']))
        
        TEMPLATE_URI = pubsub_message["template_uri"]
        PROJECT_ID = pubsub_message["project_id"]
        TARGET_TABLE = pubsub_message["target_table"]
        MODEL_URI = pubsub_message["model_uri"]
        SCALER_URI = pubsub_message.get("scaler_uri", None)

        execute_notebook(
            template_uri=TEMPLATE_URI,
            project_id=PROJECT_ID,
            target_table=TARGET_TABLE,
            model_uri=MODEL_URI,
            scaler_uri=SCALER_URI
        )
    except Exception as e:
        print(event['data'])
        logging.exception('Error occured')

def execute_notebook(
    template_uri: str, project_id: str, target_table: str, model_uri: str, scaler_uri: str
):
    import gcsfs
    import nbformat
    from nbconvert.preprocessors import ExecutePreprocessor

    fs = gcsfs.GCSFileSystem()
    with fs.open(template_uri, "r") as f:
        str_nb = f.read()

    str_nb = str_nb.replace('{{TEMPLATE_URI}}', template_uri)
    str_nb = str_nb.replace('{{PROJECT_ID}}', project_id)
    str_nb = str_nb.replace('{{TARGET_TABLE}}', target_table)
    str_nb = str_nb.replace('{{MODEL_URI}}', model_uri)
    str_nb = str_nb.replace('{{SCALER_URI}}', scaler_uri)

    nb = nbformat.reads(str_nb, nbformat.NO_CONVERT)

    ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
    ep.preprocess(nb)
