#All necessary functions
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
def read_schema(schema_arg):
    dict_type={
        "StringType()":StringType(),
        "IntegerType()":IntegerType(),
        "DoubleType()":DoubleType()
    }
    split_val=schema_arg.split(",")
    schema=StructType()
    for i in split_val:
        x= i.split(" ")
        schema.add(x[0],dict_type[x[1]],True)
    return schema
