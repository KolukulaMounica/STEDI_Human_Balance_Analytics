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

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1686877877691 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mothikikm/customers/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCuratedZone_node1686877877691",
)

# Script generated for node Step Trainer Landing Node
StepTrainerLandingNode_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mothikikm/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLandingNode_node1",
)

# Script generated for node Renamed keys for Join Customer Node
RenamedkeysforJoinCustomerNode_node1686887021979 = ApplyMapping.apply(
    frame=CustomerCuratedZone_node1686877877691,
    mappings=[
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("birthDay", "string", "`(right) birthDay`", "string"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "`(right) shareWithPublicAsOfDate`",
            "bigint",
        ),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "`(right) shareWithResearchAsOfDate`",
            "bigint",
        ),
        ("registrationDate", "bigint", "`(right) registrationDate`", "bigint"),
        ("customerName", "string", "`(right) customerName`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("lastUpdateDate", "bigint", "`(right) lastUpdateDate`", "bigint"),
        ("phone", "string", "`(right) phone`", "string"),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "`(right) shareWithFriendsAsOfDate`",
            "bigint",
        ),
    ],
    transformation_ctx="RenamedkeysforJoinCustomerNode_node1686887021979",
)

# Script generated for node Join Customer Node
JoinCustomerNode_node1686877732575 = Join.apply(
    frame1=StepTrainerLandingNode_node1,
    frame2=RenamedkeysforJoinCustomerNode_node1686887021979,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="JoinCustomerNode_node1686877732575",
)

# Script generated for node Drop Fields
DropFields_node1686878328752 = DropFields.apply(
    frame=JoinCustomerNode_node1686877732575,
    paths=[
        "`(right) serialNumber`",
        "`(right) birthDay`",
        "`(right) shareWithPublicAsOfDate`",
        "`(right) shareWithResearchAsOfDate`",
        "`(right) registrationDate`",
        "`(right) customerName`",
        "`(right) email`",
        "`(right) lastUpdateDate`",
        "`(right) phone`",
        "`(right) shareWithFriendsAsOfDate`",
    ],
    transformation_ctx="DropFields_node1686878328752",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686878328752,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-mothikikm/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
