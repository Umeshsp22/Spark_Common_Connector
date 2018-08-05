package com.connector.sources

import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Row}
import org.apache.spark.sql._
class FileReader {


  def filesProperties(DFR:DataFrameReader,format:String="",path:String="")= {
    format match {

      case "parquet" => DFR.parquet(path)
      case "orc" => DFR.orc(path)
      case "json" => DFR.json(path)
      case "text" => DFR.text(path)
      case "textFile" => DFR.textFile(path)
    }
  }

    def writeConfig(dw:DataFrameWriter[Row],cfg:Map[String,String])={
      var result=dw
      for((key,value)<-cfg){
        key match {
          case "mode" =>result.mode(value)
          case "partitionBy" =>result.partitionBy(value)
          case "sortBy"=>result=result.sortBy(value)
          case path if path!="path" => result=result.option(key,value)
          case _=>"No Match Found"
        }
      }
      result
    }

}
