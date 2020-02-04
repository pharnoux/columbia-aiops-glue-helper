import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "twitter-data", table_name = "twitter_state_selected", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = <your_database>, table_name = <your_table>, transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("id", "long", "id", "long"), ("text", "string", "text", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("id", "long", "id", "long"), ("text", "string", "text", "string")], transformation_ctx = "applymapping1")


## This is to convert a Dynamic Frame into a DataFrame first and finally a RDD 
applymapping1_rdd = applymapping1.toDF().rdd

## This is the sorted Word Count code
counts_rdd = applymapping1_rdd.flatMap(lambda line: line["text"].split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b) \
             .sortBy(lambda x: -x[1]) 

## This is to merge all the files into one
counts_df = counts_rdd.coalesce(1)

## This is to convert an RDD into DataFrame first and finally Dynamic Frame             
counts_dynamicframe = DynamicFrame.fromDF(counts_df.toDF(), glueContext, "counts")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://aiops-2020/data-lake/pierre"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = counts_dynamicframe, connection_type = "s3", connection_options = {"path": <your_path_to_s3>}, format = "csv", transformation_ctx = "datasink2")
job.commit()