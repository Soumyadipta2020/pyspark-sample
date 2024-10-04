# ============================================================================ #
# Docker
# ============================================================================ #

# Create new container - "docker run -it --name pyspark-test --mount type=bind,source=C:/Users/soumy/OneDrive/Coding,target=/app/data apache/spark-py /opt/spark/bin/pyspark"
# Reopen the older container - "docker start -ai pyspark-test"

# ============================================================================ #
# Spark Session
# ============================================================================ #

# Spark Session (Already exist in docker container)
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("demo").getOrCreate()

# ============================================================================ #
# Library
# ============================================================================ #

from pyspark.sql.functions import col, to_date, year, lower, lit, substring, \
    weekofyear, date_add, next_day, round, when

# ============================================================================ #
# Create spark DataFrame
# ============================================================================ #

df_sample = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", None),
    ],
    ["first_name", "age"],
)

df_sample.show()

# ============================================================================ #
# Read csv from web (5 years stocks data)
# ============================================================================ #

import string
from pyspark import SparkFiles
url_df = "https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/all_stocks_5yr.csv"
sc.addFile(url_df)
filePath  = 'file://' + SparkFiles.get('all_stocks_5yr.csv')
all_stocks_5yr = spark.read.csv(filePath, header=True, inferSchema= True)
all_stocks_5yr.show(3)

# ============================================================================ #
# Read csv from web (Cities bank data)
# ============================================================================ #

data_file_https_url = "https://gist.githubusercontent.com/aakashjainiitg/dbb668c58839d68d7903f508bf55043c/raw/1feec07802b4f53aceac450fa1aee5a87d9276e0/cities_data_bank.csv"
sc.addFile(data_file_https_url)
filePath_1  = 'file://' + SparkFiles.get('cities_data_bank.csv')
citiesDf = spark.read.csv(filePath_1, header=True, inferSchema= True)
citiesDf.show(3)

# ============================================================================ #
# Read from local drive (Docker external location mount is mandatory)
# ============================================================================ #

csv_file_path = '/app/data/pyspark/pyspark-sample/nim_output_final.csv'
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
df.show(3)

# ============================================================================ #
# Filter
# ============================================================================ #

## Filter data (String filter)
df_temp = df.filter((df.favored_option == "A") & (df.option_opinion == "For"))
df_temp.show(5)

## Filter data (Date filter)
all_stocks_5yr_temp = all_stocks_5yr.withColumn("date", to_date("date", "yyyy-MM-dd"))
### to check the column types
all_stocks_5yr_temp.schema 

### Filter year >= 2016
all_stocks_5yr_temp = all_stocks_5yr_temp.filter(year(col('date')) >= 2016) 
all_stocks_5yr_temp.show(5)

## Filter for patterns

### Contains a word
df_temp = df.filter(col('title').contains('birth')) 
df_temp.show(5)

### Contains a pattern
df_temp = df.filter(col('title').like("%bir%")) 
df_temp.show(5)

### Start with a pattern
df_temp = df.filter(col('title').startswith("The")) 
df_temp.show(5)

### End with a pattern
df_temp = df.filter(col('title').endswith("et")) 
df_temp.show(5)

### Doesn't contains a pattern
df_temp = df.filter(~lower(col('opinion_strength')).like("%strong%")) 
df_temp.show(5)

### Multiple patterns filter
df_temp = df.filter(
    (lower(col('opinion_strength')).like("%strong%") | 
    lower(col('opinion_strength')).like("%mil%")) & 
    col('title').startswith("The") & 
    col('option_opinion').contains("For")
) 
df_temp.show(5)

## Filter NA

### Doesn't contains NA
df_temp = df_sample.filter(col("age").isNotNull()) 
df_temp.show(5)

### Contains NA
df_temp = df_sample.filter(col("age").isNull()) 
df_temp.show(5)

## Filter withrespect to distinct entries from another dataframe
df_1 = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", None),
    ],
    ["first_name", "age"],
)

df_2 = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("boby", 75),
        ("heon", None),
    ],
    ["first_name", "age"],
)

unique_values = df_2.select("first_name").distinct().rdd.flatMap(lambda x: x).collect()
df_temp = df_1.filter(col("first_name").isin(unique_values))
df_temp.show(5)

# ============================================================================ #
# Create new column
# ============================================================================ #

# Constant value & Substring
df_temp = df_sample.withColumn("city", lit("Kolkata")).\
    withColumn("pin", lit(700001)).\
    withColumn("name_prefix", substring("first_name", 1, 2))
df_temp.show(5)

# Week start from date column, column operation, if else operation
df_temp = all_stocks_5yr.\
    withColumn("week_start", date_add(next_day(col("date"), "monday"), -7)).\
    withColumn("range", round(col("high") - col("low"), 2)).\
    withColumn("range_ind", when(col("range") > 1, "High").\
               when((col("range") <= 1) & (col("range") >= 0.5), "Medium").otherwise("Low"))
df_temp.show(5)

