package com.connector.sources

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.DataStreamReader

import scala.collection.immutable.Map


object ReaderWriter {


  def apply(readcon:DataStreamReader,cfg:Map[String,String])={
    val SetReadcon=readcon
    for((key,value)<-cfg if key!="path" && key!="format"){
      SetReadcon.option(key,value)
     }
     SetReadcon.load()
  }

  def getSparkSession={
   val Utils(spark)=Utils.spark
    spark
 }


  def apply(readcon:DataFrameReader,cfg:Map[String,String])= {
    val SetReadcon = readcon
    for ((key, value) <- cfg if key != "path" && key != "format") {
      SetReadcon.option(key, value)
    }
    if (cfg.get("path")==None) SetReadcon.load() else SetReadcon.load(cfg.get("path").get)
  }



  def apply(dw:DataFrameWriter[Row],cfg:Map[String,String])={
    if(cfg.get("path")==None) dw.save() else dw.save(cfg.get("path").get)
   }


  def reader=(config:Map[String,String])=>{
     Utils(config,config.get("format").get)
  }:Dataset[_]



  def writer=(df:DataFrame,config:Map[String,String])=>{
    Utils(df,config,config.get("format").get)
  }:Unit



}
