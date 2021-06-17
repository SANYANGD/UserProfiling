package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

class Constellation {

  def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"birthday":{"cf":"cf", "col":"birthday", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_user"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"constellation":{"cf":"user", "col":"constellation", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("Constellation")
    .master("local[10]")
    .getOrCreate()

  val readDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val transCon = (date: String) => {
    val bir = date.split('-')
    val month = bir.apply(1)
    val day = date.apply(2).toInt
    month match {
      case "01"=> if(day<21) "摩羯座" else "水瓶座"
      case "02"=> if(day<20) "水瓶座" else "双鱼座"
      case "03"=> if(day<21) "双鱼座" else "白羊座"
      case "04"=> if(day<21) "白羊座" else "金牛座"
      case "05"=> if(day<22) "金牛座" else "双子座"
      case "06"=> if(day<22) "双子座" else "巨蟹座"
      case "07"=> if(day<23) "巨蟹座" else "狮子座"
      case "08"=> if(day<24) "狮子座" else "处女座"
      case "09"=> if(day<24) "处女座" else "天秤座"
      case "10"=> if(day<24) "天秤座" else "天蝎座"
      case "11"=> if(day<23) "天蝎座" else "射手座"
      case "12"=> if(day<22) "射手座" else "摩羯座"
    }
  }

  spark.udf.register("transCon",transCon)

  readDF.createTempView("tb")

  val result = spark.sql("select id, transCon(birthday) as constellation from tb")
    .toDF()

  def constellationWrite={

    readDF.show()
    result.show()

    try{

      result.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("constellationWrite finish")

    }



    spark.close()
  }

}
