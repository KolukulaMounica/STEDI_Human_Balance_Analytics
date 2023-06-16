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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1686877877691 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mothikikm/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1686877877691",
)

# Script generated for node Accelerometer Landing Node
AccelerometerLandingNode_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mothikikm/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingNode_node1",
)

# Script generated for node Join Customer Node
JoinCustomerNode_node1686877732575 = Join.apply(
    frame1=AccelerometerLandingNode_node1,
    frame2=CustomerTrustedZone_node1686877877691,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerNode_node1686877732575",
)

# Script generated for node Drop Fields
DropFields_node1686878328752 = DropFields.apply(
    frame=JoinCustomerNode_node1686877732575,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1686878328752",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686878328752,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-mothikikm/customers/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
