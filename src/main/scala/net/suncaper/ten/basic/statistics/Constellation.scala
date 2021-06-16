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

  var result = readDF

//  val dayArr = Array[Int](20, 19, 21, 20, 21, 22, 23, 23, 23, 24, 23, 22)
//  val constellationArr = Array[String]("摩羯座", "水瓶座", "双鱼座", "白羊座", "金牛座",
//    "双子座", "巨蟹座", "狮子座", "处女座", "天秤座", "天蝎座", "射手座", "摩羯座")
//  def getConstellation(month: Int, day: Int): String = {
//    if (day < dayArr(month - 1)) constellationArr(month - 1)
//    else constellationArr(month)
//  }
//  def conCon():Array[Array[String]]={
//    var i = 0
//    var t = Array.ofDim[String](result.length, 2)
//    for (i <- 0 to result.length){
//      var month = result(i)(1).toString.split("-")(1).toInt
//      var day = result(i)(1).toString.split("-")(2).toInt
//      t(i)(1) = getConstellation(month, day)
//    }
//    println(t)
//    t
//  }

  def constellationWrite={
    readDF.show()
    result.show()


//    result.show()
//
//    result.write
//      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()

    spark.close()
  }

}
