# Create new container - "docker run -it --name pyspark-test --mount type=bind,source=C:/Users/soumy/OneDrive/Coding,target=/app/data apache/spark-py /opt/spark/bin/pyspark"
# Reopen the older container - "docker start -ai pyspark-test"

# Spark Session (Already exist in docker container)
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("demo").getOrCreate()

# Library
from pyspark.sql.functions import col, to_date, year

# Create spark DataFrame
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

# Read csv from web (5 years stocks data)
import string
from pyspark import SparkFiles
url_df = "https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/all_stocks_5yr.csv"
sc.addFile(url_df)
filePath  = 'file://' + SparkFiles.get('all_stocks_5yr.csv')
all_stocks_5yr = spark.read.csv(filePath, header=True, inferSchema= True)
all_stocks_5yr.show()

# Read csv from web (Cities bank data)
data_file_https_url = "https://gist.githubusercontent.com/aakashjainiitg/dbb668c58839d68d7903f508bf55043c/raw/1feec07802b4f53aceac450fa1aee5a87d9276e0/cities_data_bank.csv"
sc.addFile(data_file_https_url)
filePath_1  = 'file://' + SparkFiles.get('cities_data_bank.csv')
citiesDf = spark.read.csv(filePath_1, header=True, inferSchema= True)
citiesDf.show()

# Read from local drive (Docker external location mount is mandatory)
csv_file_path = '/app/data/pyspark/pyspark-sample/nim_output_final.csv'
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
df.show()

# Filter
## Filter data (String filter)
df_temp = df.filter((df.favored_option == "A") & (df.option_opinion == "For"))
df_temp.show(5)

## Filter data (Date filter)
all_stocks_5yr_temp = all_stocks_5yr.withColumn("date", to_date("date", "yyyy-MM-dd"))
all_stocks_5yr_temp.schema # to check the column types
all_stocks_5yr_temp = all_stocks_5yr_temp.filter(year(col('date')) >= 2016) # Filter year >= 2016
all_stocks_5yr_temp.show()

## Filter for patterns
df_temp = df.filter(col('title').contains('birth')) # contains a word
df_temp.show(5)

df_temp = df.filter(col('title').like("%bir%")) # contains a pattern
df_temp.show(5)

df_temp = df.filter(col('title').startswith("The")) # start with a pattern
df_temp.show(5)

df_temp = df.filter(col('title').endswith("et")) # end with a pattern
df_temp.show(5)

