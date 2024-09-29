# Create new container - "docker run -it --name pyspark-test apache/spark-py /opt/spark/bin/pyspark"
# Reopen the older container - "docker start -ai pyspark-test"

# Spark Session (Already exist in docker container)
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("demo").getOrCreate()

# Create DataFrame
df = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", 13),
    ],
    ["first_name", "age"],
)

df.show()

# Read csv from web
import string
from pyspark import SparkFiles
data_file_https_url = "https://gist.githubusercontent.com/aakashjainiitg/dbb668c58839d68d7903f508bf55043c/raw/1feec07802b4f53aceac450fa1aee5a87d9276e0/cities_data_bank.csv"
sc.addFile(data_file_https_url)
filePath  = 'file://' +SparkFiles.get('cities_data_bank.csv')
citiesDf = spark.read.csv(filePath, header=True, inferSchema= True)
citiesDf.show()