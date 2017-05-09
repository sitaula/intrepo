// Databricks notebook source
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

val ssc = new StreamingContext(sc, Seconds(1))
val lines = ssc.textFileStream("/mnt/ss/test")
val words = lines.flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)
wordCounts.print()

ssc.start
ssc.awaitTermination



// COMMAND ----------

ssc.stop()

// COMMAND ----------

val test = sc.textFile("/mnt/ss/test/test")

// COMMAND ----------

test.count()

// COMMAND ----------

test.show()

// COMMAND ----------

test.head

// COMMAND ----------

test.first

// COMMAND ----------

ssc

// COMMAND ----------

val test = spark.read.csv("/mnt/ss/member.csv")

// COMMAND ----------

test.first

// COMMAND ----------

test.count()

// COMMAND ----------

test.columns

// COMMAND ----------

test.groupBy("_c0")

// COMMAND ----------

test.groupBy("_c0").count().count()

// COMMAND ----------

ssc

// COMMAND ----------

sc

// COMMAND ----------

sc.getConf.getAll

// COMMAND ----------

