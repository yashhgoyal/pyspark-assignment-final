# Data frame assignment.py
import os
import sys

# FIX: Set environment variables for PySpark compatibility with Python 3.11+
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYTHONHASHSEED'] = '0'

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataFrame Assignment") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.executor.heartbeatInterval", "10s") \
    .config("spark.network.timeout", "100s") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("="*70)
print("DATA FRAME ASSIGNMENT - COMPLETE SOLUTIONS")
print("="*70)

# ============ EASY QUESTIONS ============
print("\n" + "="*70)
print("EASY QUESTIONS")
print("="*70)

# Question 1 - is_adult
print("\n1. Conditional Column - is_adult:")
employees = [(1, "AJAY", 28), (2, "VIJAY", 35), (3, "MANOJ", 22)]
schema1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
df1 = spark.createDataFrame(employees, schema1)
df1 = df1.select(
    col("id"),
    col("name"),
    col("age"),
    when(col("age") >= 18, "TRUE").otherwise("FALSE").alias("is_adult"))
df1.show()


# Question 2 - Pass/Fail
print("\n2. Categorizing Values - Pass/Fail:")
grades = [(1, 85), (2, 42), (3, 73)]
schema2 = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("score", IntegerType(), True)
])
df2 = spark.createDataFrame(grades, schema2)
df2 = df2.select(
    col("student_id"),
    col("score"),
    when(col("score") >= 50, "Pass").otherwise("Fail").alias("grade")
)
df2.show()

print("\n3. Multiple Conditions - Category:")
transactions = [(1, 1000), (2, 200), (3, 5000)]
schema3 = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])
df3 = spark.createDataFrame(transactions, schema3)
df3 = df3.select(
    col("transaction_id"),
    col("amount"),
    when(col("amount") > 1000, "High")
    .when((col("amount") >= 500) & (col("amount") <= 1000), "Medium")
    .otherwise("Low").alias("category")
)
df3.show()

# Question 4 - price_range
print("\n4. Conditional Column with String Values - price_range:")
products = [(1, 30.5), (2, 150.75), (3, 75.25)]
schema4 = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("price", DoubleType(), True)
])
df4 = spark.createDataFrame(products, schema4)
df4 = df4.select(
    col("product_id"),
    col("price"),
    when(col("price") < 50, "Cheap")
    .when((col("price") >= 50) & (col("price") <= 100), "Moderate")
    .otherwise("Expensive").alias("price_range") )
df4.show()

# Question 5 - is_holiday
print("\n5. Conditional Column with Date - is_holiday:")
events = [(1, "2024-07-27"), (2, "2024-12-25"), (3, "2025-01-01")]
schema5 = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("date", StringType(), True)
])
df5 = spark.createDataFrame(events, schema5)
df5 = df5.select(
    col("event_id"),
    col("date"),
    when((col("date") == "2024-12-25") | (col("date") == "2025-01-01"), "TRUE")
    .otherwise("FALSE").alias("is_holiday"))
df5.show()

# Question 1 - stock_level
print("\n1. Nested Conditional Column - stock_level:")
inventory = [(1, 5), (2, 15), (3, 25)]
schema6 = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True)
])
df6 = spark.createDataFrame(inventory, schema6)
df6 = df6.select(
    col("item_id"),
    col("quantity"),
    when(col("quantity") < 10, "Low")
    .when((col("quantity") >= 10) & (col("quantity") <= 20), "Medium")
    .otherwise("High").alias("stock_level"))
df6.show()

# Question 2 - email_provider
print("\n2. Conditional String Manipulation - email_provider:")
customers = [(1, "john@gmail.com"), (2, "jane@yahoo.com"), (3, "doe@hotmail.com")]
schema7 = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("email", StringType(), True)
])
df7 = spark.createDataFrame(customers, schema7)
df7 = df7.select(
    col("customer_id"),
    col("email"),
    when(col("email").contains("gmail"), "Gmail")
    .when(col("email").contains("yahoo"), "Yahoo")
    .otherwise("Other").alias("email_provider"))
df7.show()