# -*- coding: utf-8 -*-
"""schemas.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1sbmkk-YPLvTE8xT9aW1M-KgR04Js2hgS

# Schemas

## Download and install Spark
"""

!ls

!apt-get update
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
!tar xf spark-2.3.1-bin-hadoop2.7.tgz
!pip install -q findspark

"""## Setup environment"""

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.3.1-bin-hadoop2.7"

import findspark
findspark.init()
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate() 
spark

"""## Downloading and preprocessing Chicago's Reported Crime Data"""

!wget https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD

!mv rows.csv\?accessType\=DOWNLOAD reported-crimes.csv

from pyspark.sql.functions import to_timestamp,col,lit
df = spark.read.csv('reported-crimes.csv',header=True).withColumn('Date',to_timestamp(col('Date'),'MM/dd/yyyy hh:mm:ss a')).filter(col('Date') <= lit('2018-11-11'))
df.show(5)

"""## Schemas"""

ls

df.printSchema()

df.columns

df.head(2)

df.show(2)

df.select(['ID','Date']).show(6)

df.withColumnRenamed('Date','Time')

df.show(4)

ArrestCount = df.groupBy('Arrest').count()

df.printSchema()

ls

df.groupBy('Location').count().orderBy('count',ascending=False).show(3)

ArrestCount.show()

val1 = ArrestCount.collect()[0][1]
val2 =  ArrestCount.collect()[1][1]
percent = (val2/(val1+val2))*100
perce

percent

df.filter(col('Arrest')== 'false').count() / df.select('Arrest').count()
