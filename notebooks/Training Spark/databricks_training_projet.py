# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1: Mount an Azure Blob storage container

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://greathouse1bronze@storagedatabrickssulan.blob.core.windows.net",
  mount_point = "/mnt/greathouse1bronze",
  extra_configs = {"fs.azure.account.key.storagedatabrickssulan.blob.core.windows.net":dbutils.secrets.get(scope = "AKV", key = "storagedatabrickssulankey1")})

# COMMAND ----------

dbutils.fs.ls("/mnt/greathouse1bronze/")

# COMMAND ----------

# Filename = "Real Estate Ad"
# csvFile = "/mnt/greathouse1bronze/" + Filename + ".csv"

# csvDF=(spark.read                    # The DataFrameReader
#    .option("sep", ";")         # Use tab delimiter (default is comma-separator)
#    .option("header", "true")   # Use first line of all files as header
#    .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
# )             
# display(csvDF)
# csvDF.printSchema()

# COMMAND ----------

# # UDF read csv and write parquet 
# def read_write(csvFile, csvSchema, DestFile):
#     csvDF = (spark.read
#       .option('header', 'true')
#       .option('sep', ",")
#       .schema(csvSchema)
#       .csv(csvFile)
#     )
#     csvDF = csvDF.distinct().dropna()
#     display(csvDF)
    
#     (csvDF.write                       # Our DataFrameWriter
#       .option("compression", "snappy") # One of none, snappy, gzip, and lzo
#       .mode("overwrite")               # Replace existing files
#       .parquet(DestFile)               # Write DataFrame to Parquet files
#     )
#     return 1
# read_write_udf = udf(read_write)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read and understand Data

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stock Roger&Brothers

# COMMAND ----------

from pyspark.sql.types import *
csvSchema = StructType([
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("Rooms", FloatType(), True),
    StructField("Bedrooms", FloatType(), True),
    StructField("price", StringType(), True),
    StructField("house id", StringType(), True)
])

Filename = "Stock Roger&Brothers"
csvFile = "/mnt/greathouse1bronze/" + Filename + ".csv"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"

# read_write_udf(csvFile, csvSchema, DestFile)

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', ";")
  .schema(csvSchema)
  .csv(csvFile)
)
csvDF = csvDF.distinct()
display(csvDF)

(csvDF.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(DestFile)               # Write DataFrame to Parquet files
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stock GreatHouse Holdings

# COMMAND ----------

from pyspark.sql.types import *
csvSchema = StructType([
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("Rooms", IntegerType(), True),
    StructField("Bedrooms", IntegerType(), True),
    StructField("price", StringType(), True),
    StructField("house id", StringType(), True)
])

Filename = "Stock GreatHouse Holdings"
csvFile = "/mnt/greathouse1bronze/" + Filename + ".csv"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"

# read_write_udf(csvFile, csvSchema, DestFile)

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', ";")
  .schema(csvSchema)
  .csv(csvFile)
)
csvDF = csvDF.distinct()
display(csvDF)

(csvDF.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(DestFile)               # Write DataFrame to Parquet files
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Real Estate Ad

# COMMAND ----------

from pyspark.sql.types import *
csvSchema = StructType([
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("housing_median_age", FloatType(), True),
    StructField("total_rooms", FloatType(), True),
    StructField("total_bedrooms", FloatType(), True),
    StructField("population", FloatType(), True),
    StructField("households", FloatType(), True),
    StructField("median_income", FloatType(), True),    
    StructField("median_house_value", FloatType(), True),  
    StructField("ocean_proximity", StringType(), True)
])

Filename = "Real Estate Ad"
csvFile = "/mnt/greathouse1bronze/" + Filename + ".csv"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"

# read_write_udf(csvFile, csvSchema, DestFile)

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', ",")
  .schema(csvSchema)
  .csv(csvFile)
)
csvDF = csvDF.distinct().dropna()
display(csvDF)

(csvDF.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(DestFile)               # Write DataFrame to Parquet files
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### GreatHouse Holdings District

# COMMAND ----------

from pyspark.sql.types import *
csvSchema = StructType([
  StructField("longitude", FloatType(), False),
  StructField("latitude", FloatType(), False),
])

Filename = "GreatHouse Holdings District"
csvFile = "/mnt/greathouse1bronze/" + Filename + ".csv"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', ";")
  .schema(csvSchema)
  .csv(csvFile)
)
csvDF = csvDF.distinct()
display(csvDF)

(csvDF.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(DestFile)               # Write DataFrame to Parquet files
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Roger&Brothers District

# COMMAND ----------

from pyspark.sql.types import *
csvSchema = StructType([
  StructField("longitude", FloatType(), False),
  StructField("latitude", FloatType(), False),
])

Filename = "Roger&Brothers District"
csvFile = "/mnt/greathouse1bronze/" + Filename + ".csv"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', ";")
  .schema(csvSchema)
  .csv(csvFile)
)
csvDF = csvDF.distinct()
display(csvDF)

(csvDF.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(DestFile)               # Write DataFrame to Parquet files
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: clean and create datasets
# MAGIC * Union District and Stock, create distinct id and company id
# MAGIC * 2 tables : House Distinct and House Stock
# MAGIC * For House Stock, we delete outline points: the ones with number of rooms >= 1000 or with price < 10000

# COMMAND ----------

Filename = "Roger&Brothers District"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"
df1 = spark.read.parquet(DestFile)

Filename = "GreatHouse Holdings District"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"
df2 = spark.read.parquet(DestFile)

df = df1.union(df2)
df = df.distinct()
df = df.withColumn("distinct id", monotonically_increasing_id()+1)

# ----------
Filename = "Real Estate Ad"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"
REA = spark.read.parquet(DestFile)
REA = REA.distinct()

df = df.join(REA, ["longitude", "latitude"], how="left")

##------- save -----
Filename = "House Distinct"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"
(df.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(DestFile)               # Write DataFrame to Parquet files
)

print(df1.count(), df2.count(), df.count())
del df, df1, df2, REA

# COMMAND ----------

##------------- distinct
Filename = "House Distinct"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"
dst = spark.read.parquet(DestFile)
dst = dst.select("distinct id", "longitude", "latitude").distinct()

##------------- Stock

Filename = "Stock Roger&Brothers"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"
df1 = spark.read.parquet(DestFile)
df1 = df1.withColumn("company id", lit("RB"))

Filename = "Stock GreatHouse Holdings"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"
df2 = spark.read.parquet(DestFile)
df2 = df2.withColumn("company id", lit("GH"))

df = df1.union(df2)
df = df.distinct()

##----------------
##- clean --------
##----------------
# df.describe().show()
# tranformer le price as float => price_float
df = df.withColumn("price", regexp_replace("price", ",", ".") )
df = df.withColumn("price", col("price").cast(FloatType()))

# delete outlines (57) 
df = df.filter(df.Rooms < 1000)
df = df.filter(df.price >= 10000)

# do a left join to get district id and drop longtitude, latitude
df = df.join(dst, ["longitude", "latitude"], how="left").drop("longitude", "latitude")

display(df)

##------- save -----

Filename = "House Stock"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"
(df.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(DestFile)               # Write DataFrame to Parquet files
)

print(df1.count(), df2.count(), df.count())

del df, df1, df2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Analyse 

# COMMAND ----------

import pyspark.sql.functions as f
import matplotlib.pyplot as plt
import pandas as pd

# del df

# COMMAND ----------

# Filename = "Real Estate Ad"
Filename = "House Stock"
DestFile = "/mnt/greathouse2silver/" + Filename + ".parquet"
df = spark.read.parquet(DestFile)
# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse the average price per number of rooms

# COMMAND ----------

stats_av_by_rooms = df.groupby("Rooms").agg(f.mean("price").alias("Avg_price_per_nb_rooms"))
stats_av_by_rooms = stats_av_by_rooms.orderBy("Rooms").toPandas()
stats_av_by_rooms.plot(kind='bar', legend=None, title = "Average price per number of rooms")
del stats_av_by_rooms

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse the different indicators the lowest, highest, average of price from each district 

# COMMAND ----------

display(
df.groupBy("distinct id").agg(
    f.min("price").alias("Lowest_price_per_distinct"),
    f.max("price").alias("Highest_price_per_distinct"),
    f.mean("price").alias("Avg_price_per_distinct"),
    f.countDistinct('house id').alias("Number_of_house_distinct")  
))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Which kind of house is most in the market (such as how many rooms, how many bedrooms)

# COMMAND ----------

## add information to dataFrame
distinctDF = spark.read.parquet("/mnt/greathouse2silver/" + "House Distinct" + ".parquet")
distinctDF = distinctDF.select("distinct id", "ocean_proximity")
df = df.join(distinctDF, ["distinct id"], how="left")
df = df.distinct()

# COMMAND ----------

dfc = df.drop("house id", "distinct id")

categ = dfc.select('company id').distinct().rdd.flatMap(lambda x:x).collect()
exprs = [F.when(F.col('company id') == cat,1).otherwise(0).alias("company" + str(cat)) for cat in categ]
dfc = dfc.select(exprs+dfc.columns)

del categ, exprs

categ = dfc.select('ocean_proximity').distinct().rdd.flatMap(lambda x:x).collect()
exprs = [F.when(F.col('ocean_proximity') == cat,1).otherwise(0).alias("ocean_proximity_" + str(cat)) for cat in categ]
dfc = dfc.select(exprs+dfc.columns)

dfc = dfc.drop("ocean_proximity_None", "ocean_proximity", 'company id')

del categ, exprs

# COMMAND ----------

from pyspark.mllib.stat import Statistics
df_rdd = dfc.rdd.map(lambda row: row[0:])
corr_mat = Statistics.corr(df_rdd, method="pearson")
corr_mat_df = pd.DataFrame(corr_mat, columns=dfc.columns, index=dfc.columns)
del dfc, df_rdd, corr_mat

# COMMAND ----------

display(corr_mat_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### By the correration matrix, we can see the price has a correlation postif with "ocean_proximity_<1H" and "ocean_proximity_NEAR BAY", then with "Rooms" and "Bedrooms"

# COMMAND ----------

# MAGIC %md
# MAGIC #### For different price range, analyse the difference of houses (nb of rooms, bedrooms)

# COMMAND ----------

df_percent =  ( df.select("house id", "price").distinct().agg(
    f.percentile_approx("price", [0, 0.25, 0.5, 0.75, 1], 1000000).alias("Percentile")
))

# COMMAND ----------

df_percent=df_percent.select( f.explode(df_percent.Percentile).alias("Percentile") ).collect()

# COMMAND ----------

from pyspark.sql.functions import when
df = df.withColumn("price_range",  
              when(df.price < df_percent[1][0], "1")
              .when(df.price < df_percent[2][0],"2")
              .when(df.price < df_percent[3][0] ,"3")
              .otherwise("4")
             ) 

# COMMAND ----------

display(
df.groupBy("price_range").agg(
    f.mean("Rooms").alias("Avg_number_Rooms"),
    f.mean("Bedrooms").alias("Avg_number_Rooms"),
    f.mean("price").alias("Avg_price"),
    f.countDistinct('house id').alias("Number_of_house")  
))

# COMMAND ----------

total = df.filter(f.col("price_range")==1).count()
stats_range1 = df.filter(f.col("price_range")==1).groupBy("ocean_proximity").agg(f.countDistinct('house id')/total)

total = df.filter(f.col("price_range")==2).count()
stats_range2 = df.filter(f.col("price_range")==2).groupBy("ocean_proximity").agg(f.countDistinct('house id')/total)

total = df.filter(f.col("price_range")==3).count()
stats_range3 = df.filter(f.col("price_range")==3).groupBy("ocean_proximity").agg(f.countDistinct('house id')/total)

total = df.filter(f.col("price_range")==4).count()
stats_range4 = df.filter(f.col("price_range")==4).groupBy("ocean_proximity").agg(f.countDistinct('house id')/total)

# COMMAND ----------

# stats_range1.toPandas().plot(kind='bar', legend=None, title = "Average price per number of rooms")
stats_range1.show()
stats_range2.show()
stats_range3.show()
stats_range4.show()
