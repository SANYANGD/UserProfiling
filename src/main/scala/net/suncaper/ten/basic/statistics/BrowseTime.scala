package net.suncaper.ten.basic.statistics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

class BrowseTime {

  def catalog =
    s"""{
       |  "table":{"namespace":"default", "name":"tbl_logs"},
       |  "rowkey":"id",
       |  "columns":{
       |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |    "log_time":{"cf":"cf", "col":"log_time", "type":"string"},
       |    "global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"}
       |  }
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_log"},
       |"rowkey":"id",
       |"columns":{
       |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |  "browseTime":{"cf":"log", "col":"browseTime", "type":"string"}
       |}
       |}""".stripMargin


  val spark = SparkSession.builder()
    .appName("browseTime")
    .master("local[10]")
    .getOrCreate()

  import spark.implicits._

  val source: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val login = unix_timestamp(min("log_time")).as("login")

  val logout = unix_timestamp(max("log_time")).as("logout")

  val sum = count("log_time").as("sum")


  var result = source
    .withColumn("day",dayofyear($"log_time"))
    .groupBy("global_user_id","day")
    .agg(login, logout, sum)

  result = result.select($"global_user_id",$"sum".cast("int"), $"login".cast("int"),$"logout".cast("int"),
      when((($"logout"-$"login")/60/$"sum") <= 1, "1分钟内")
        .when(((($"logout"-$"login")/60/$"sum") > 1) and ((($"logout"-$"login")/60/$"sum") <= 5), "1-5分钟")
        .otherwise("5分钟以上")
        .as("browseTime"))
    .withColumnRenamed("global_user_id","id")
      .drop("login")
      .drop("logout")



  def browseTimeWrite={
    source.show()
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

      println("browseTimeWrite finish")

    }

    spark.close()
  }

}
