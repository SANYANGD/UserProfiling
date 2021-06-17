package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

class LastLogin {

  def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"lastLoginTime":{"cf":"cf", "col":"lastLoginTime", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_beh"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"lastLogin":{"cf":"behavior", "col":"lastLogin", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("shc test")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val readDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()
   val recencyCol = datediff(date_sub(current_timestamp(), 660), from_unixtime('lastLoginTime)) as "temp"

  val lastLoginW = readDF
    .select('id,
      when(recencyCol< 24*60*60, "1天内")
        .when(recencyCol< 7*60*60 , "7天内")
        .when(recencyCol < 14*60*60, "14天内")
        .when(recencyCol < 30*60*60, "30天内")
        .as("lastLogin"))


  def lastLoginWrite={
    readDF.show()
    lastLoginW.show()

    try{

      lastLoginW.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("lastLoginWrite finish")

    }



    spark.close()
  }

}
