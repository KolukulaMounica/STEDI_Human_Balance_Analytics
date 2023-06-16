import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mothikikm/customers/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Exclude PII
ExcludePII_node1686777869876 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ExcludePII_node1686777869876",
)

# Script generated for node Cutsomer Trusted
CutsomerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ExcludePII_node1686777869876,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-mothikikm/customers/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CutsomerTrusted_node3",
)

job.commit()
