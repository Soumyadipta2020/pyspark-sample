# ============================================================================ #
# Docker
# ============================================================================ #

# ============================================================================ #
# apache/spark-py
# ============================================================================ #

# Create new container - "docker run -it --name pyspark-test --mount type=bind,source=C:/Users/soumy/OneDrive/Coding,target=/app/data apache/spark-py /opt/spark/bin/pyspark"
# Reopen the older container - "docker start -ai pyspark-test"

# ============================================================================ #
# Spark Session
# ============================================================================ #

# Spark Session (Already exist in docker container)
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("demo").getOrCreate()
# spark.conf.set("spark.executor.instances", 2)
# spark.conf.set("spark.executor.cores", 2)

# ============================================================================ #
# Library
# ============================================================================ #

from pyspark.sql.functions import col, to_date, year, lower, lit, substring, \
    weekofyear, date_add, next_day, round, when, regexp_replace, avg, max, upper, \
    expr

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
).repartition(2)

df_sample.show()

# ============================================================================ #
# Read csv from web (5 years stocks data)
# ============================================================================ #

import string
from pyspark import SparkFiles
url_df = "https://raw.githubusercontent.com/plotly/datasets/refs/heads/master/all_stocks_5yr.csv"
sc.addFile(url_df)
filePath  = 'file://' + SparkFiles.get('all_stocks_5yr.csv')
all_stocks_5yr = spark.read.csv(filePath, header=True, inferSchema= True).repartition(10)
all_stocks_5yr.show(3)

# ============================================================================ #
# Read csv from web (Cities bank data)
# ============================================================================ #

data_file_https_url = "https://gist.githubusercontent.com/aakashjainiitg/dbb668c58839d68d7903f508bf55043c/raw/1feec07802b4f53aceac450fa1aee5a87d9276e0/cities_data_bank.csv"
sc.addFile(data_file_https_url)
filePath_1  = 'file://' + SparkFiles.get('cities_data_bank.csv')
citiesDf = spark.read.csv(filePath_1, header=True, inferSchema= True).repartition(10)
citiesDf.show(3)

# ============================================================================ #
# Read from local drive (Docker external location mount is mandatory)
# ============================================================================ #

csv_file_path = '/app/data/pyspark/pyspark-sample/nim_output_final.csv'
df = spark.read.csv(csv_file_path, header=True, inferSchema=True).repartition(10)
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
all_stocks_5yr_temp.printSchema() 

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
).repartition(2)

df_2 = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("boby", 75),
        ("heon", None),
    ],
    ["first_name", "age"],
).repartition(2)

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

# Week start from date column, column operation, if else operation, replace a pattern, 
# uppercase & lowercase
df_temp = all_stocks_5yr.\
    withColumn("week_start", date_add(next_day(col("date"), "monday"), -7)).\
    withColumn("range", round(col("high") - col("low"), 2)).\
    withColumn("range_ind", when(col("range") > 1, "High").\
               when((col("range") <= 1) & (col("range") >= 0.5), "Medium").otherwise("Low")).\
    withColumn("week_start_new", regexp_replace(col("week_start"), "-", "/")).\
    withColumn("range_ind", upper(col("range_ind"))).\
    withColumn("Name", lower(col("Name")))
df_temp.show(5)

# ============================================================================ #
# Replace pattern in column names
# ============================================================================ #
old_columns = df_temp.columns
new_columns = [c.replace('_', ' ') for c in old_columns]

# Use select() to rename the columns
df_temp = df_temp.select(*[col(c).alias(new_c) for c, new_c in zip(old_columns, new_columns)])
df_temp.show(5)

# ============================================================================ #
# Number of rows & columns
# ============================================================================ #

# Get the number of rows
num_rows = df_temp.count()

# Get the number of columns
num_cols = len(df_temp.columns)

# Print the results
print("Number of rows:", num_rows)
print("Number of columns:", num_cols)

# ============================================================================ #
# Rename columns
# ============================================================================ #
df_temp = df_temp.\
    withColumnRenamed("week start new", "week start excel").\
    withColumnRenamed("range ind", "Variance level").\
    withColumnRenamed("range", "Variance")
df_temp.show(5)

# ============================================================================ #
# Select columns 
# ============================================================================ #

# Without renaming
df_temp_new = df_temp.select(
    "date", "variance"
)
df_temp_new.show(5)

# With renaming
df_temp_new = df_temp.select(
    col("date").alias("Date"),
    col("variance").alias("Var")
)
df_temp_new.show(5)

# ============================================================================ #
# Exploratory Data Analysis
# ============================================================================ #

# Summary Statistics
df_temp.describe().show()

# Summary Statistics by other variable
df_temp.groupBy("week start").\
    agg(
        round(avg("open"), 2).alias("Average open"),
        round(avg("high"), 2).alias("Average high"),
        round(avg("low"), 2).alias("Average low"),
        round(avg("close"), 2).alias("Average close"),
        round(avg("volume"), 2).alias("Average volume"),
        round(max("Variance"), 2).alias("Max Variance")
    ).\
    show(5)

# ============================================================================ #
# User Defined Function
# ============================================================================ #
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def range_category(var):
    if var > 1:
        return "High"
    elif var >= 0.5 and var <= 1:
        return "Medium"
    else:
        return "Low"

range_category_udf = udf(range_category, StringType())

df_temp = all_stocks_5yr.\
    withColumn("range", round(col("high") - col("low"), 2)).\
    withColumn("range_ind", when(col("range") > 1, "High").\
               when((col("range") <= 1) & (col("range") >= 0.5), "Medium").otherwise("Low")).\
    withColumn("range_ind_udf", range_category_udf(col("range")))
df_temp.show(5)

# ============================================================================ #
# Check number of partitions
# ============================================================================ #
num_partitions = df_temp.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

# ============================================================================ #
# Pivot Longer
# ============================================================================ #

# Pivot longer function
def pivot_longer(df, id_cols, names_to, values_to):
    from pyspark.sql import functions as F
    id_columns = id_cols
    # Dynamically retrieve the value columns to be unpivoted
    value_columns = [col for col in df.columns if col not in id_columns]
    # Number of columns to stack
    n_cols = len(value_columns)
    # Dynamically build the stack expression
    stack_expr = f"stack({n_cols}, " + ", ".join([f"'{col}', {col}" for col in value_columns]) + ") as (" + names_to + "," + values_to + ")"
    df = df.select(*id_columns, F.expr(stack_expr))
    return df

# Pivot longer implementation
unPivotDF = all_stocks_5yr.\
    withColumn("volume", col("volume").cast("double"))
unPivotDF = pivot_longer(unPivotDF, ["date", "Name"], "Metric", "Value")
unPivotDF.show(5)

# ============================================================================ #
# Pivot wider
# ============================================================================ #

pivotDF = unPivotDF.groupBy("date", "Name").pivot("Metric").sum("Value")
pivotDF.show(5)

# ============================================================================ #
# Apply same function to all numeric columns
# ============================================================================ #

# Define apply function for implementation 
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import List

def apply_function_to_numeric_columns(
    df: DataFrame,
    function, 
    *args, 
    **kwargs) -> DataFrame:
    """
    Applies a given function to all numeric columns in a PySpark DataFrame.
    Args:
        df (DataFrame): The input PySpark DataFrame.
        function: The function to apply to the numeric columns.
        *args: Additional positional arguments to pass to the function.
        **kwargs: Additional keyword arguments to pass to the function.
    Returns:
        DataFrame: The PySpark DataFrame with the function applied to the 
                   numeric columns.
    """
    numeric_cols = [
        c for c, t in df.dtypes if t in ["int", "double", "float", "long"]
    ]
    for column in numeric_cols:
        df = df.withColumn(column, function(col(column), *args, **kwargs))
    return df

# Function to apply
def add_one(x):
    return x/100

# Implementation
df_modified = apply_function_to_numeric_columns(pivotDF, add_one)
df_modified.show(5)
