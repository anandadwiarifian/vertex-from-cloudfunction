def executeNotebookCustomModel(
    template_uri: str, project_id: str, target_bucket: str, 
    model_uri: str, scaler_uri: str, organization_id: str,
    bucket_pipeline: str, datafoundation_table: str, topic_id: str,
    audience_id: int
):
    import gcsfs
    import nbformat
    from nbconvert.preprocessors import ExecutePreprocessor

    fs = gcsfs.GCSFileSystem()
    with fs.open(template_uri, "r") as f:
        str_nb = f.read()

    str_nb = str_nb.replace('{{TEMPLATE_URI}}', template_uri)
    str_nb = str_nb.replace('{{PROJECT_ID}}', project_id)
    str_nb = str_nb.replace('{{TARGET_BUCKET}}', target_bucket)
    str_nb = str_nb.replace('{{ORGANIZATION_ID}}', organization_id)
    str_nb = str_nb.replace('{{BUCKET_PIPELINE}}', bucket_pipeline)
    str_nb = str_nb.replace('{{DATAFOUNDATION_TABLE}}', datafoundation_table)
    str_nb = str_nb.replace('{{MODEL_URI}}', model_uri)

    str_nb = str_nb.replace('{{SCALER_URI}}', scaler_uri)
    str_nb = str_nb.replace('{{AUDIENCE_ID}}', str(audience_id))
    str_nb = str_nb.replace('{{TOPIC_ID}}', topic_id)

    nb = nbformat.reads(str_nb, nbformat.NO_CONVERT)

    ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
    ep.preprocess(nb)