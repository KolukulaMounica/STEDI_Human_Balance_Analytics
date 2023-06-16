import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1686877877691 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://udacity-mothikikm/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="AccelerometerTrustedZone_node1686877877691",
    )
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mothikikm/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrustedZone_node1",
)

# Script generated for node Join Machine Learning Node
JoinMachineLearningNode_node1686877732575 = Join.apply(
    frame1=StepTrainerTrustedZone_node1,
    frame2=AccelerometerTrustedZone_node1686877877691,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="JoinMachineLearningNode_node1686877732575",
)

# Script generated for node Drop Fields
DropFields_node1686878328752 = DropFields.apply(
    frame=JoinMachineLearningNode_node1686877732575,
    paths=["sensorReadingTime"],
    transformation_ctx="DropFields_node1686878328752",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.getSink(
    path="s3://udacity-mothikikm/accelerometer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node3",
)
MachineLearningCurated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node3.setFormat("json")
MachineLearningCurated_node3.writeFrame(DropFields_node1686878328752)
job.commit()
