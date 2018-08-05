package com.connector.sources

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader}
import org.apache.spark.sql._

import scala.collection.immutable.Map

object Utils {

  var spark =SparkSession.builder.getOrCreate()
  val format_1 = Array("csv", "cassandra", "jdbc", "hive", "ignite", "memsql")
  val format_2 = Array("json", "orc", "textFile", "parquet", "text")


  val stream: String => DataStreamReader = (format: String) => spark.readStream.format(format)
 // val writeStream = (df: DataFrame, format: String) => df.writeStream.format(format)

  val reader: String => org.apache.spark.sql.DataFrameReader = (format: String) => spark.read.format(format)
  val writer:(DataFrame,String)=>DataFrameWriter[Row] = (df: DataFrame, format: String) => df.write.format(format)



  def apply(config: Map[String, String], format: String = ""):Dataset[_]    = {
    format match {
      case "kafka" => ReaderWriter(stream(format), config)
      case input_1 if format_1.contains(input_1.split("/.").last) => ReaderWriter(reader(format), config)
      case input_2 if format_2.contains(input_2) => new FileReader().filesProperties(reader(""), config.get("format").get, config.get("path").get)
      case _ => ReaderWriter(reader(format), config)
    }
  }


  def apply(df: DataFrame, config: Map[String, String], format: String) = {
    format match {
      //case "kafka" => writeStream(df, format)
      case input_1 if format_1.contains(input_1.split("/.").last) => ReaderWriter(new FileReader().writeConfig(writer(df, format),config),config)
      case input_2 if format_2.contains(input_2) => {
        input_2 match {
          case "parquet" => new FileReader().writeConfig(writer(df, ""), config).parquet(config.get("path").get)
          case "orc" => new FileReader().writeConfig(writer(df, ""), config).orc(config.get("path").get)
          case "json" => new FileReader().writeConfig(writer(df, ""), config).json(config.get("path").get)
          case "text" => new FileReader().writeConfig(writer(df, ""), config).text(config.get("path").get)
        }
      }
    }
  }

  def unapply(spark:SparkSession):Option[SparkSession]={
    Some(spark)
  }
}
