import pyspark 

from pyspark import SparkContext 
from pyspark.sql import SQLContext

from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate();

from pyspark.sql.types import *

from datetime import date 

current_date = date.today()

file_name = str(current_date)

bucket_name = "gs://<bucket_name>"

from pyspark.context import SparkContext
spark = SparkSession(sc)

car_data = spark.read.csv(bucket_name+"/car_purchase_data/"+file_name+".csv",header=True)

car_data1 = car_data.filter(car_data['state'] != 'null')

car_data2 = car_data1.filter(car_data1['country'] != 'null')

car_data3 = car_data2.dropDuplicates(['email', 'date'])

car_data4 = car_data3.withColumn("id", car_data3["id"].cast(IntegerType())).withColumn("cost", car_data3["cost"].cast(FloatType()))

car_data5 = car_data4.withColumn("date",car_data4['date'].cast(DateType()))

car_purchase_clean_data_output = car_data5

output_dir=bucket_name+"/car_purchase_clean_data_output/"+file_name+"_car_data"

car_purchase_clean_data_output.coalesce(1).write.format("json").save(output_dir)
