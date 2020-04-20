# PySpark-Essential-Training Google COLABS
Handling datasets/dataframes in PySpark

----------------------------------------------------
# Download Spark Files and Install PySpark 
// Run these codes in google Colab

!apt-get update

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

!wget -q http://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz

!tar xf spark-2.3.1-bin-hadoop2.7.tgz

!pip install -q findspark

---------------------------------------------------
# Start Spark Sesison

// Run these codes in google Colab to setup a new Spark Session.
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

--------------------------------------------------

# Download a Dataset into Spark

!wget https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD

//Here I am using Chicago Crime Dataset (1.5GB)


--------------------------------------------------

# Create a Dataframe using the downloaded Data


df = spark.read.csv('reported-crimes.csv',header=True)


---------------------------------------------------

# Some basic function on the Dataframe in Pyspark


df.show(5) //Shows top 5 rows


df.printSchema() // Prints the Schema of the Dataframe

df.columns //Shows the column names

df.head(5) //Shows top 5 rows as a list

df.select(['ID','Date']).show(5) //select few columns


----------------------------------------------------
# Advanced function on Dataframe

df.groupBy('Location Description').count().orderBy('count',ascending=False).show(3) //Top 3 Locations by cases reported.


OUTPUT 
+--------------------+-------+
|Location Description|  count|
+--------------------+-------+
|              STREET|1770576|
|           RESIDENCE|1144628|
|           APARTMENT| 698159|
+--------------------+-------+

# Pyspark Function

from pyspark.sql.functions import min, max, date_add, upper, lower, substring //import the function you need here

df.select(upper(col('Location Description'))).show(2) //Convert to upper case

df.select(lower(col('Location Description'))).show(2) //Convert to Lower case

df.select(substring(col('Location Description'),1,5)).show(2) //Extract a substring

df.select(max(col('Date'))).show(1) //Select Max of date

df.select(date_add(max(col('Date')),3)).show(1) //Add date 

# Joins

Read another dataset to join:

!wget https://data.cityofchicago.org/api/views/z8bn-74gv/rows.csv?accessType=DOWNLOAD

ps = spark.read.csv('police.csv',header=True)


Padding (make two columns similiar) >

from pyspark.sql.functions import lpad

ps = ps.withColumn('Padded_ID',lpad(col('DISTRICT'),3,'0'))

ps.show(5)

<< Joining >>

df.join(ps, df.District == ps.Padded_ID, 'left_outer').show(5)

//Completed
