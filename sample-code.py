# Create new container - "docker run -it --name pyspark-test apache/spark-py /opt/spark/bin/pyspark"
# Reopen the older container - "docker start -ai pyspark-test"

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo").getOrCreate()

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
