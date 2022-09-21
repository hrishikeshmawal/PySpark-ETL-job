import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window





def main():

    spark = initialize_Spark()

    df_with_schema = loadDFWithSchema(spark)

    df_cleaned = clean_data(df_with_schema)

    aggregations_and_loading(df_cleaned)

    print("Done!", "\n")



def initialize_Spark():

    spark = SparkSession.builder \
        .appName("Simple etl job") \
        .getOrCreate()

    print("Spark Initialized", "\n")

    return spark


def loadDFWithSchema(spark):

    schema = StructType() \
      .add("Object Number",IntegerType(),True) \
      .add("Is Highlight",BooleanType(),True) \
      .add("Is Timeline Work",BooleanType(),True) \
      .add("Is Public Domain",BooleanType(),True) \
      .add("Object ID",IntegerType(),True) \
      .add("Gallery Number",StringType(),True) \
      .add("Department",StringType(),True) \
      .add("AccessionYear",DoubleType(),True) \
      .add("Object Name",StringType(),True) \
      .add("Title",StringType(),True) \
      .add("Culture",StringType(),True) \
      .add("Period",StringType(),True) \
      .add("Dynasty",StringType(),True) \
      .add("Reign",StringType(),True) \
      .add("Portfolio",StringType(),True) \
      .add("Constituent ID",IntegerType(),True) \
      .add("Artist Role",StringType(),True) \
      .add("Artist Prefix",StringType(),True) \
      .add("Artist Display Name",StringType(),True) \
      .add("Artist Display Bio",StringType(),True)\
      .add("Artist Suffix",StringType(),True) \
      .add("Artist Alpha Sort",StringType(),True)\
      .add("Artist Nationality",StringType(),True) \
      .add("Artist Begin Date",IntegerType(),True)\
      .add("Artist End Date",IntegerType(),True) \
      .add("Artist Gender",StringType(),True)\
      .add("Artist ULAN URL",IntegerType(),True) \
      .add("Artist Wikidata URL",StringType(),True)\
      .add("Object Date",IntegerType(),True) \
      .add("Object Begin Date",IntegerType(),True)\
      .add("Object End Date",IntegerType(),True) \
      .add("Medium",StringType(),True)\
      .add("Dimensions",StringType(),True) \
      .add("Credit Line",StringType(),True)\
      .add("Geography Type",StringType(),True)\
      .add("City",StringType(),True) \
      .add("State",StringType(),True)\
      .add("County",StringType(),True)\
      .add("Country",StringType(),True) \
      .add("Region",StringType(),True)\
      .add("Subregion",StringType(),True)\
      .add("Locale",StringType(),True) \
      .add("Locus",StringType(),True)\
      .add("Excavation",StringType(),True)\
      .add("River",StringType(),True) \
      .add("Classification",StringType(),True)\
      .add("Rights and Reproduction",StringType(),True)\
      .add("Link Resource",IntegerType(),True) \
      .add("Object Wikidata URL",StringType(),True)\
      .add("Metadata Date",StringType(),True)\
      .add("Repository",StringType(),True) \
      .add("Tags",StringType(),True)\
      .add("Tags AAT URL",StringType(),True)\
      .add("Tags Wikidata URL",StringType(),True)



    df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("MetObjects.txt")

    print("Data loaded into PySpark", "\n")

    return df_with_schema


def clean_data(df_with_schema):

    #Dimensions cleaning
    regex_string= "\d{1,3}\.\d{1,2} +x \d{1,3}\.\d{1,2} +x \d{1,3}\.\d{1,2} cm{1}|\d{1,3}\.\d{1,2} +x \d{1,3}\.\d{1,2} cm{1}"
    df_with_schema=df_with_schema.withColumn("Dimensions_cleaned1",regexp_extract(col("Dimensions"),regex_string,0))
    df_with_schema=df_with_schema.withColumn("Dimensions_cleaned",regexp_replace(col("Dimensions_cleaned1")," cm",""))


    # 3 new columns with the extracted value of height, width and length in cm
    df_with_schema = df_with_schema.withColumn('height_col', split(df_with_schema['Dimensions_cleaned'], ' x ').getItem(0)) \
                    .withColumn('width_col', split(df_with_schema['Dimensions_cleaned'], ' x ').getItem(1)) \
                    .withColumn('length_col', split(df_with_schema['Dimensions_cleaned'], ' x ').getItem(2))

    
    #cleaning country column
    df_with_schema=df_with_schema.withColumn("Country_cleaned",split(df_with_schema.Country,"or|\\|"))

    #Forward and backward fill: Constituent ID column
    w1 = Window.partitionBy('Title').orderBy('Object ID').rowsBetween(Window.unboundedPreceding,0)
    w2 = w1.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df_with_schema=df_with_schema.withColumn('Constituent ID_cleaned', coalesce(last('Constituent ID',True).over(w1), first('Constituent ID',True).over(w2)))
    df_final=df_with_schema.withColumn("Country_cleaned",df_with_schema.Country_cleaned.cast(StringType()))

    print("Data cleaned", "\n")

    return df_final


def aggregations_and_loading(df_final):

    #saving aggregation of Artworks per individual country
    Artworks_individual_country=df_final.groupBy("Country_cleaned").count().orderBy(col("count").desc())
    Artworks_individual_country.coalesce(1).write.option("header","true").csv("data/table1")

    #saving artists per country
    No_artists_country=df_final.groupBy("Country_cleaned").agg(count("Artist Display Name").alias("count")).sort(desc("count"))
    No_artists_country.coalesce(1).write.option("header","true").csv("data/table2")

    #Average_height_width_length
    Average_height_width_length =df_final.groupBy("Country_cleaned").agg({"height_col":"mean","width_col":"mean","length_col":"mean"})
    Average_height_width_length.coalesce(1).write.option("header","true").csv("data/table3")
    
    #Collect a unique list of constituent ids per country
    unique_list_of_constituent_ids=df_final.groupBy("Country_cleaned").agg(countDistinct("Constituent ID_cleaned").alias("distinct_Constituent ID")).sort(desc("distinct_Constituent ID"))
    unique_list_of_constituent_ids.coalesce(1).write.option("header","true").csv("data/table4")

    print("Data loaded", "\n")


    

if __name__ == '__main__':
    main()