package net.suncaper.ten.basic.matching

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

class BrowsePage {

  def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_logs"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
       |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_log"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"browsePage":{"cf":"log", "col":"browsePage", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("browsePage")
    .master("local[10]")
    .getOrCreate()

  val source: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()


  val BrowsePage = (date: String) => {
    var result:String = null
    if (date.contains("index")){
      result = "首页"
    }else if(date.contains("toMemberLogin")||(date.contains("Login")&&date.contains("member"))){
      result = "登录页"
    }else if(date.contains("itemList")){
      result = "分类页"
    }else if(date.contains("product")){
      result = "商品页"
    }else if(date.contains("orderList")||date.contains("myorder")){
      result = "我的订单页"
    }else{
      result = "其他"
    }
    result
  }
  spark.udf.register("BrowsePage",BrowsePage)

  source.createOrReplaceTempView("bp")

  var result = spark.sql("""select global_user_id,browsePage(loc_url) from bp""")
      .withColumnRenamed("UDF:BrowsePage(loc_url)", "browsePage")
      .withColumnRenamed("global_user_id", "id")


  result = result.where("browsePage != '其他'")
    .groupBy("id", "browsePage").agg(count("*").as("num")).drop("num")

//  val window:WindowSpec = Window.partitionBy("id").orderBy("num")
//
//  result = result.select("id","BrowsePage","dense_rank() over window")
//    //.where("rank = 1").drop("rank")

  def browsePageWrite={
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

      println("browsePageWrite finish")

    }

    spark.close()
  }


}
