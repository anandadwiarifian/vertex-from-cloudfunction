def createDataprocJob(
    project_id: str, pyspark_script: str, 
    custom_image_tag: str, parameters: dict
):
    from datetime import datetime
    SERVICE_ACCOUNT = "serviceaccountnyavertex@project-for-sda-development.iam.gserviceaccount.com"
    REGION = "asia-southeast2"
    TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
    BUCKET_NAME = "vertex-testing-poc-123"
    BUCKET_URI = f"gs://{BUCKET_NAME}"
    PIPELINE_ROOT = "{}/pipeline_root/generic_pyspark".format(BUCKET_URI)
    BATCH_ID = "generic-pyspark-" + TIMESTAMP
    SUBNETWORK_URI = "projects/project-for-sda-development/regions/asia-southeast2/subnetworks/default"
    BUCKET_TEMP = "vertex-testing-poc-123/generic_lookalike/temp"

    import google.cloud.aiplatform as aiplatform
    from kfp import dsl
    from kfp.v2 import compiler

    aiplatform.init(project=project_id, staging_bucket=BUCKET_URI, location=REGION)
    SCRIPT_ARG = []
    for key, value in parameters.items():
        SCRIPT_ARG.append(f"--{key}")
        SCRIPT_ARG.append(value)

    SCRIPT_ARG.append("--bucket")
    SCRIPT_ARG.append(BUCKET_TEMP)

    import tempfile
    tmpdir = tempfile.gettempdir()

    @dsl.pipeline(
        name="generic-lookalike-pyspark",
    )
    def pipeline(
        main_python_file_uri: str,
        args: list,
        project_id: str,
        container_image: str,
        batch_id: str = BATCH_ID,
        location: str = REGION,   
        service_account: str = SERVICE_ACCOUNT
    ):
        from google_cloud_pipeline_components.v1.dataproc import \
            DataprocPySparkBatchOp

        _ = DataprocPySparkBatchOp(
            project=project_id,
            location=location,
            main_python_file_uri=main_python_file_uri,
            service_account=service_account,
            args=args,
            batch_id=batch_id,
            container_image=container_image,
            subnetwork_uri=SUBNETWORK_URI,

        )

    compiler.Compiler().compile(pipeline_func=pipeline, package_path=f"{tmpdir}/pipeline.json")

    pipeline = aiplatform.PipelineJob(
        display_name="generic-lookalike-pyspark",
        template_path=f"{tmpdir}/pipeline.json",
        pipeline_root=PIPELINE_ROOT,
        enable_caching=False,
        parameter_values={
            "main_python_file_uri": pyspark_script,
            "args": SCRIPT_ARG,
            "container_image": custom_image_tag,
            "project_id": project_id,
        }
    )

    pipeline.run(sync=False)