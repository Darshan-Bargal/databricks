// Databricks notebook source
// MAGIC %md
// MAGIC # Download elasticsearch-spark JAR and create library in Databricks
// MAGIC 1. Download the appropriate version of the [ES-Hadoop package](https://www.elastic.co/downloads/hadoop).
// MAGIC 1. Extract `elasticsearch-spark-xx_x.xx-x.x.x.jar` from the zip file. For example, `elasticsearch-spark-20_2.11-6.2.3.jar`.
// MAGIC 1. Create the library in your Databricks Workspace and attach to one or more clusters.
// MAGIC 
// MAGIC # Documentation
// MAGIC * [ES-Hadoop documentation](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/float.html)
// MAGIC * [Release Notes](https://www.elastic.co/guide/en/elasticsearch/hadoop/6.2/eshadoop-6.2.3.html)
// MAGIC * [Elasticsearch configuration](https://www.elastic.co/guide/en/elasticsearch/hadoop/master/configuration.html)
// MAGIC * [Elasticsearch Spark documentation](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html)

// COMMAND ----------

// MAGIC %md ## Load data into DBFS
// MAGIC The data set used here contains [American Kennel Club dog breed data](https://data.world/len/dog-canine-breed-size-akc/workspace/file?filename=AKC+Breed+Info.csv).

// COMMAND ----------

// MAGIC %md
// MAGIC **Important:** In the following cells, replace `<ip-address>` and `<port>` with your Elasticsearch configuration and `<my>` with your Elasticsearch URL.

// COMMAND ----------

// DBTITLE 1,Test connectivity to your Elasticsearch instance
// MAGIC %sh 
// MAGIC nc -vz 107.23.84.198 9200

// COMMAND ----------

// MAGIC %md For a successful connection, the response will be: `<ip-address> <port> is open`.

// COMMAND ----------

// DBTITLE 1,Download data set
// MAGIC %sh wget -O /tmp/akc_breed_info.csv https://query.data.world/s/msmjhcmdjslsvjzcaqmtreu52gkuno

// COMMAND ----------

// DBTITLE 1,Copy to DBFS
// MAGIC %sh cp /tmp/akc_breed_info.csv /dbfs/mnt/data/

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC ls -lah /dbfs/mnt/data/

// COMMAND ----------

// MAGIC %sh
// MAGIC mkdir -p  /dbfs/mnt/data

// COMMAND ----------

// MAGIC %md ## Write data to Elasticsearch<br>

// COMMAND ----------

// DBTITLE 1,Delete existing index, if exists
// MAGIC %sh 
// MAGIC curl -XDELETE 'http://107.23.84.198:9200/index'?

// COMMAND ----------

// DBTITLE 1,Set Elasticsearch URL
val esURL = "107.23.84.198"

// COMMAND ----------

// DBTITLE 1,Read CSV file and save to Elasticsearch
val df = spark.read.option("header","true").csv("/mnt/data/akc_breed_info.csv")

df.write
  .format("org.elasticsearch.spark.sql")
  .option("es.nodes.wan.only","true")
  .option("es.port","443")
  .option("es.net.ssl","true")
  .option("es.nodes", esURL)
  .mode("Overwrite")
  .save("index/dogs")

// COMMAND ----------

// DBTITLE 1,Verify data was loaded
// MAGIC %sh curl https://<my>.elasticsearch.com/index/dogs/_search?q=Breed:Collie

// COMMAND ----------

// MAGIC %md ## Read data from Elasticsearch using Spark

// COMMAND ----------

// DBTITLE 1,Use Data Frame API to access Elasticsearch index
val reader = spark.read
  .format("org.elasticsearch.spark.sql")
  .option("es.nodes.wan.only","true")
  .option("es.port","443")
  .option("es.net.ssl","true")
  .option("es.nodes", esURL)

val df = reader.load("index/dogs").na.drop.orderBy($"breed")
display(df)

// COMMAND ----------

// DBTITLE 1,Use SQL to access Elasticsearch index
// MAGIC %sql
// MAGIC drop table if exists dogs;
// MAGIC 
// MAGIC create temporary table dogs
// MAGIC using org.elasticsearch.spark.sql
// MAGIC options('resource'='index/dogs', 
// MAGIC   'nodes'= '<my>.elasticsearch.com',
// MAGIC   'es.nodes.wan.only'='true',
// MAGIC   'es.port'='443',
// MAGIC   'es.net.ssl'='true');
// MAGIC   
// MAGIC select weight_range as size, count(*) as number 
// MAGIC from (
// MAGIC   select case 
// MAGIC     when weight_low_lbs between 0 and 10 then 'toy'
// MAGIC     when weight_low_lbs between 11 and 20 then 'small'
// MAGIC     when weight_low_lbs between 21 and 40 then 'medium'
// MAGIC     when weight_low_lbs between 41 and 80 then 'large'
// MAGIC     else 'xlarge' end as weight_range
// MAGIC   from dogs) d
// MAGIC group by weight_range
