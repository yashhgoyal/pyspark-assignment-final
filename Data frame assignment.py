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
    .otherwise("Other").alias("email_provider")
)
df7.show()

# Question 3 - season
print("\n3. Conditional Date Manipulation - season:")
orders = [(1, "2024-07-01"), (2, "2024-12-01"), (3, "2024-05-01")]
schema8 = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True)
])
df8 = spark.createDataFrame(orders, schema8)
df8 = df8.withColumn("month", month(col("order_date")))
df8 = df8.select(
    col("order_id"),
    col("order_date"),
    when(col("month").isin([6, 7, 8]), "Summer")
    .when(col("month").isin([12, 1, 2]), "Winter")
    .otherwise("Other").alias("season"))
df8.show()


# Question 4 - discount
print("\n4. Multiple Nested Conditions - discount:")
sales = [(1, 100), (2, 1500), (3, 300)]
schema9 = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])
df9 = spark.createDataFrame(sales, schema9)
df9 = df9.select(
    col("sale_id"),
    col("amount"),
    when(col("amount") < 200, 0)
    .when((col("amount") >= 200) & (col("amount") <= 1000), 10)
    .otherwise(20).alias("discount")
)
df9.show()

# Question 5 - is_morning
print("\n5. Conditional Column with Boolean Values - is_morning:")
logins = [(1, "09:00"), (2, "18:30"), (3, "14:00")]
schema10 = StructType([
    StructField("login_id", IntegerType(), True),
    StructField("login_time", StringType(), True)
])
df10 = spark.createDataFrame(logins, schema10)
df10 = df10.select(
    col("login_id"),
    col("login_time"),
    when(col("login_time") < "12:00", "TRUE").otherwise("FALSE").alias("is_morning")
)
df10.show()

# ============ COMPLEX QUESTIONS ============
print("\n" + "="*70)
print("COMPLEX QUESTIONS")
print("="*70)

# Question 1 - employee category
print("\n1. Complex Nested Conditions - employee category:")
employees_data = [(1, 25, 30000), (2, 45, 50000), (3, 35, 40000)]
schema11 = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True)])
df11 = spark.createDataFrame(employees_data, schema11)
df11 = df11.select(
    col("employee_id"),
    col("age"),
    col("salary"),
    when((col("age") < 30) & (col("salary") < 35000), "Young & Low Salary")
    .when((col("age") >= 30) & (col("age") <= 40) & (col("salary") >= 35000) & (col("salary") <= 45000), "Middle Aged & Medium Salary")
    .otherwise("Old & High Salary").alias("category"))
df11.show()

# Question 2 - feedback and is_positive
print("\n2. Multiple Conditional Columns - feedback and is_positive:")
reviews = [(1, 1), (2, 4), (3, 5)]
schema12 = StructType([
    StructField("review_id", IntegerType(), True),
    StructField("rating", IntegerType(), True)])
df12 = spark.createDataFrame(reviews, schema12)
df12 = df12.select(
    col("review_id"),
    col("rating"),
    when(col("rating") < 3, "Bad")
    .when((col("rating") == 3) | (col("rating") == 4), "Good")
    .otherwise("Excellent").alias("feedback"),
    when(col("rating") >= 3, "TRUE").otherwise("FALSE").alias("is_positive"))
df12.show()

# Question 3 - content_category
print("\n3. Complex String Conditions - content_category:")
documents = [(1, "The quick brown fox"),
    (2, "Lorem ipsum dolor sit amet"),
    (3, "Spark is a unified analytics engine")]
schema13 = StructType([
    StructField("doc_id", IntegerType(), True),
    StructField("content", StringType(), True)])
df13 = spark.createDataFrame(documents, schema13)
df13 = df13.select(
    col("doc_id"), col("content"),
    when(col("content").contains("fox"), "Animal Related")
    .when(col("content").contains("Lorem"), "Placeholder Text")
    .when(col("content").contains("Spark"), "Tech Related")
    .otherwise("Other").alias("content_category"))
df13.show()


# Question 4 - task_duration
print("\n4. Multiple Date Conditions - task_duration:")
tasks = [(1, "2024-07-01", "2024-07-10"),
    (2, "2024-08-01", "2024-08-15"),
    (3, "2024-09-01", "2024-09-05") ]
schema14 = StructType([
    StructField("task_id", IntegerType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True) ])
df14 = spark.createDataFrame(tasks, schema14)
df14 = df14.withColumn("duration_days", datediff(col("end_date"), col("start_date")))
df14 = df14.select(
    col("task_id"), col("start_date"), col("end_date"),
    when(col("duration_days") <= 7, "Short")
    .when((col("duration_days") > 7) & (col("duration_days") <= 14), "Medium")
    .otherwise("Long").alias("task_duration") )
df14.show()

# Question 5 - order_type
print("\n5. Multiple Conditions - order_type:")
orders_data = [(1, 5, 100), (2, 10, 150), (3, 20, 300)]
schema15 = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_price", IntegerType(), True) ])
df15 = spark.createDataFrame(orders_data, schema15)
df15 = df15.select(
    col("order_id"), col("quantity"), col("total_price"),
    when((col("quantity") < 10) & (col("total_price") < 200), "Small & Cheap")
    .when((col("quantity") >= 10) & (col("total_price") < 200), "Bulk & Discounted")
    .otherwise("Premium Order").alias("order_type") )
df15.show()

# Question 6 - weather
print("\n6. Conditional Column with Multiple Boolean Values - weather:")
weather = [(1, 25, 60), (2, 35, 40), (3, 15, 80)]
schema16 = StructType([
    StructField("day_id", IntegerType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True) ])
df16 = spark.createDataFrame(weather, schema16)
df16 = df16.select(
    col("day_id"), col("temperature"), col("humidity"),
    when(col("temperature") > 30, "TRUE").otherwise("FALSE").alias("is_hot"),
    when(col("humidity") > 70, "TRUE").otherwise("FALSE").alias("is_humid") )
df16.show()

# Question 7 - math_grade and english_grade
print("\n7. Multiple Conditional Columns - math_grade and english_grade:")
scores = [(1, 85, 92), (2, 58, 76), (3, 72, 64)]
schema17 = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("math_score", IntegerType(), True),
    StructField("english_score", IntegerType(), True)
])
df17 = spark.createDataFrame(scores, schema17)
df17 = df17.select(
    col("student_id"), col("math_score"), col("english_score"),
    when(col("math_score") > 80, "A")
    .when((col("math_score") >= 60) & (col("math_score") <= 80), "B")
    .otherwise("C").alias("math_grade"),
    when(col("english_score") > 80, "A")
    .when((col("english_score") >= 60) & (col("english_score") <= 80), "B")
    .otherwise("C").alias("english_grade") )
df17.show()

# Question 8 - email_domain
print("\n8. Conditional Column Based on String - email_domain:")
emails = [(1, "user@gmail.com"), (2, "admin@yahoo.com"), (3, "info@hotmail.com")]
schema18 = StructType([
    StructField("email_id", IntegerType(), True),
    StructField("email_address", StringType(), True)
])
df18 = spark.createDataFrame(emails, schema18)
df18 = df18.select(
    col("email_id"),
    col("email_address"),
    when(col("email_address").contains("gmail"), "Gmail")
    .when(col("email_address").contains("yahoo"), "Yahoo")
    .otherwise("Hotmail").alias("email_domain"))
df18.show()

# Question 9 - quarter
print("\n9. Conditional Column Based on Month - quarter:")
payments = [(1, "2024-07-15"), (2, "2024-12-25"), (3, "2024-11-01")]
schema19 = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("payment_date", StringType(), True)
])
df19 = spark.createDataFrame(payments, schema19)
df19 = df19.withColumn("month", month(col("payment_date")))
df19 = df19.select(
    col("payment_id"),
    col("payment_date"),
    when(col("month").isin([1, 2, 3]), "Q1")
    .when(col("month").isin([4, 5, 6]), "Q2")
    .when(col("month").isin([7, 8, 9]), "Q3")
    .otherwise("Q4").alias("quarter"))
df19.show()
spark.stop()