def executeNotebookHeuristicPython(
    python_script_path: str, project_id: str, target_bucket: str, 
    organization_id: str, feature_list: list, audience_id: int,
    bucket_pipeline: str, datafoundation_table: str, topic_id: str,
    msisdn_sample_path: str, max_msisdn_count: int
):
    import gcsfs
    import nbformat
    from nbconvert.preprocessors import ExecutePreprocessor

    fs = gcsfs.GCSFileSystem()
    with fs.open(python_script_path, "r") as f:
        str_nb = f.read()

    str_nb = str_nb.replace('{{PROJECT_ID}}', project_id)
    str_nb = str_nb.replace('{{TARGET_BUCKET}}', target_bucket)
    str_nb = str_nb.replace('{{ORGANIZATION_ID}}', organization_id)
    str_nb = str_nb.replace('{{DATAFOUNDATION_TABLE}}', datafoundation_table)
    str_nb = str_nb.replace('{{BUCKET_PIPELINE}}', bucket_pipeline)
    str_nb = str_nb.replace('{{FEATURE_LIST}}', str(feature_list))
    str_nb = str_nb.replace('{{AUDIENCE_ID}}', str(audience_id))
    str_nb = str_nb.replace('{{TOPIC_ID}}', topic_id)
    str_nb = str_nb.replace('{{MAX_MSISDN_COUNT}}', str(max_msisdn_count))
    str_nb = str_nb.replace('{{MSISDN_SAMPLE_PATH}}', msisdn_sample_path)

    nb = nbformat.reads(str_nb, nbformat.NO_CONVERT)

    ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
    ep.preprocess(nb)