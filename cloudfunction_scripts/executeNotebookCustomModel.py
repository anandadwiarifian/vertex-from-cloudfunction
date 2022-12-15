def executeNotebookCustomModel(
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