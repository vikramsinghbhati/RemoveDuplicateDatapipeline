from pyspark.sql import SparkSession
from commonFunc import read_schema
import ConfigParser
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType,StringType
spark = SparkSession.builder.appName("duplicate_removal_datapipeline").getOrCreate()

#Reading all configs like input/output path and scheama from config.ini
config=ConfigParser.ConfigParser()
config.read(r'C:\Users\91988\PycharmProjects\Duplicate_Removal\projectconfig\config.ini')
inputLocation=config.get('paths','inputLocation')
duplicateOutputLocation=config.get('paths','duplicateDataOutputLocation')
nonDuplicateOutputLocation=config.get('paths','nonDuplicateDataOutputLocation')
inputSchemaFromConfig =config.get('Schema','landingFileSchema')
landingFileSchema = read_schema(inputSchemaFromConfig)

#reading landingFileSchema and rmove few null records preset in file

inputDF=spark.read.schema(landingFileSchema).csv(inputLocation)
cleanInputDF=inputDF.na.drop()
cleanInputDF.printSchema()
#find duplicate records from DataFrame

id_counts =cleanInputDF.groupby('emp_id').agg(func.count('*').alias('id_cnt')).filter(func.col('id_cnt')>1)

id_counts.printSchema()
# create list which contain ids which having emp_id count more than 1
dup_ids = [i.emp_id for i in id_counts.collect()]

# data with duplicate ids and write into output location path defined in config ini
dup_id_data_df = cleanInputDF.filter(func.col('emp_id').isin(dup_ids))
dup_id_data_df.write.csv(duplicateOutputLocation)
# data with non duplicate ids and write into output location path defined in config ini
non_dup_id_df = cleanInputDF.filter(func.col('emp_id').isin(dup_ids) == False)
non_dup_id_df.write.csv(nonDuplicateOutputLocation)




