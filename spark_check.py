import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql.window import Window

from pyspark.sql.functions import  (col , sum,
                                   count, min, max, avg,countDistinct,when,
                                   row_number, rank, dense_rank, lag, lead, first, last,)

spark = SparkSession.builder \
    .appName("first_spark_app") \
    .getOrCreate()

         # Create sample data for practice
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime, timedelta

# Sample employees data
employees_data = [
    (1, "Alice Johnson", "Engineering", 95000, "2020-01-15"),
    (2, "Bob Smith", "Sales", 75000, "2019-06-20"),
    (3, "Carol White", "Engineering", 105000, "2018-03-10"),
    (4, "David Brown", "Marketing", 68000, "2021-08-01"),
    (5, "Eve Davis", "Sales", 82000, "2020-11-12"),
    (6, "Frank Miller", "Engineering", 92000, "2022-02-28"),
    (7, "Grace Lee", "Marketing", 71000, "2019-09-15")
]

projects_data = [
    (101, "Website Redesign", 1),
    (102, "Mobile App", 3),
    (103, "Marketing Campaign", 4),
    (104, "Data Pipeline", 6),
    (105, "Sales Dashboard", 2)
]


employees_df = spark.createDataFrame(employees_data, ["id", "name", "department", "salary", "hire_date"])
employees_df.createOrReplaceTempView("employees")

print("Employees table created!")
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
ranked_df = employees_df.withColumn("ranked",rank().over(window_spec))
ranked_df.show()
result1 = spark.sql("""
select
name,department,salary,
CASE when salary >= 100000 THEN "High"
     when salary >= 80000  THEN "Medium"
     ELSE "Entry level"
END AS salary_catagory

from
employees

order by salary DESC

""")

print("salary categories:")
result2= spark.sql("""
select * from result1

""")
result1.show()
complex_view_query = spark.sql("""
            create or replace temp view high_performance as 
            select 
                name,
                department,
                salary,
            case 
                when salary >= 100000 then 'Top Tier'
                when salary >= 90000 then 'High Performer'
                else 'standard'
            end as performace_tier
            from employees
            where salary >= 90000
          """)

complex_view_query.show()