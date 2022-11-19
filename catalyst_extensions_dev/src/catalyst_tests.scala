// Some fun with Spark's catalyst.
// RS19112022

// REFERENCES:
// 
// https://www.youtube.com/watch?v=IlovS-Y7KUk - Extension points API
// https://medium.com/@pratikbarhate/extending-apache-spark-catalyst-for-custom-optimizations-9b491efdd24f
// Extension points: https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html
// https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/SparkSessionExtensionSuite.scala

// GOALS:
// 
// (1) Detect sensitive field used in query
// (2) Add counts for that field to query
// (3) Prevent returning results if resultset row count below sensitive threshold

val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("resources/data.csv")

df.createOrReplaceTempView("data")

val sqlQuery = """
select
    data_subject,
    count(data_subject) as total_txn,
    sum(amount) as total_amount
from data
group by 1
"""

val result = spark.sql(sqlQuery)
result.show