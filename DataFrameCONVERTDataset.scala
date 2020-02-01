package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object DataFrameCONVERTDataset {
  def main(args: Array[String]) {
    case class cc_asl(name : String, age: Int, city: String)
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameCONVERTDataset").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\CSV\\asl.csv"
    val ds = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data).as[cc_asl]
    ds.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab").as[cc_asl]
    res.show()
    spark.stop()
  }
}

/*
Summary :
--------
Converting Dataframe to Dataset.

DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
 */