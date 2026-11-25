SCRIPT_URI = "s3://cde-spark-script/artifacts/jobs/freight_route_metrics.py"
LOG_URI = "s3://cde-emr-spark-cluster-logs/"
CLUSTER_ID = "{{ ti.xcom_pull(task_ids='create_cluster') }}"
STEP_ID = "{{ ti.xcom_pull(task_ids='add_spark_step')[0] }}"


JOB_FLOW_OVERRIDES = {
    "Name": "freight-route-metrics",
    "ReleaseLabel": "emr-7.5.0",
    "Applications": [{"Name": "Spark"}],
    "LogUri": LOG_URI,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    # Safety net: if Airflow dies before terminate_cluster, EMR shuts the idle cluster down.
    "AutoTerminationPolicy": {"IdleTimeout": 3600},
    "Configurations": [
        {
            "Classification": "yarn-site",
            "Properties": {
                # In cluster deploy mode the driver runs inside the YARN AM.
                # EMR 6.x+ lets AMs land on task nodes unless labels pin them to CORE.
                "yarn.node-labels.enabled": "true",
                "yarn.node-labels.am.default-node-label-expression": "CORE",
            },
        }
    ],
    "Instances": {
        "Ec2SubnetId": "subnet-0737ad770665b10e0",
        # Steps arrive from a later task. terminate_cluster ends the cluster, not EMR.
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "InstanceGroups": [
            {
                "Name": "primary",
                "InstanceRole": "MASTER",
                "Market": "ON_DEMAND",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "core",
                "InstanceRole": "CORE",
                "Market": "ON_DEMAND",
                "InstanceType": "r5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "task",
                "InstanceRole": "TASK",
                "Market": "ON_DEMAND",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
    },
    "Tags": [
        {"Key": "project", "Value": "freight-route-metrics"},
        {"Key": "orchestrator", "Value": "airflow"},
    ],
}

SPARK_STEPS = [
    {
        "Name": "freight-route-metrics",
        # Airflow owns cleanup, so a failed step must not tear the cluster down itself.
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", "spark.dynamicAllocation.enabled=false",
                "--num-executors", "2",
                "--executor-cores", "3",
                "--executor-memory", "4g",
                "--driver-memory", "4g",
                SCRIPT_URI
                ],
        },
    }
]
