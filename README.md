Setting The pyspark libraries.
import pyspark
import os
import sys
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

from pyspark import SparkConf
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder \
      .master('local') \
      .appName('HelloFresh') \
      .config('spark.executor.memory', '2g') \
      .getOrCreate()

df = sqlContext.read.option("mode", "DROPMALFORMED").json ("D:\\hellofresh\\recipes.json").select("name" ,"ingredients", "url", "image","cookTime", "recipeYield", "datePublished", "prepTime","description")


Here DROPMALFORMED option used to drop invalid JSON elements.

new_columns = list()
for column in ["cookTime", "prepTime"]:

    # What would column look like without alphabets-- Removing alphabets from preptime and cooktime.
    col_without_alphabets = F.regexp_replace(df[column], "[a-zA-Z]", "")

    # What would column look like without numerals
    col_without_numerals = F.regexp_replace(df[column], "[0-9]", "")

    # If without numerals the column remains the same then keep as-is, else remove alphabets
    new_columns.append(F.when(col_without_numerals == df[column],
                        F.lit(0)).otherwise(col_without_alphabets).alias("New{}".format(column)))
                        
Average time calculation

newdf= df.select(["*"] + new_columns)
Preptimedf= newdf.withColumn("TotalCookTime", col("NewcookTime") + col("NewprepTime"))
Avgpreptimedf=Preptimedf.withColumn("Difficulties", when(col("TotalCookTime") < 30, lit("easy")).when((col("TotalCookTime") >= 30) & (col("TotalCookTime") < 60), lit("medium")).otherwise(lit("hard")))

Records with beef in ingredients
outpdf = Avgpreptimedf.groupBy("Difficulties").avg("TotalCookTime").write.mode(saveMode='Append').option("header","true").json(path="D:/hellofresh/Avgtime.json")
newdf.select("name" ,"ingredients", "url", "image","cookTime", "recipeYield", "datePublished",
             "prepTime","description").filter("ingredients like '%beef%'").write.mode(saveMode='Append').option("header","true").json(path="D:/hellofresh/beef.json")

