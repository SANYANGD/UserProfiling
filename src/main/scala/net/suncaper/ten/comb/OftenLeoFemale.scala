package net.suncaper.ten.comb

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

class OftenLeoFemale {
  //经常登录的狮子座女性
  //用到的表
  //user:gender
  //user:ageGroup
  //user:job
  //biz:maxOrderAmount
  //查询aft_basic_user
  def catalog1 =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_user"},
       |"rowkey":"id",
       |"columns":{
       |"user_id":{"cf":"rowkey","col":"id", "type":"string"},
       |"ageGroup":{"cf":"user","col":"ageGroup","type":"string"},
       |"birthday":{"cf":"user","col":"birthday","type":"string"},
       |"constellation":{"cf":"user","col":"constellation","type":"string"},
       |"gender":{"cf":"user","col":"gender","type":"string"},
       |"job":{"cf":"user","col":"job","type":"string"},
       |"lastAddressId":{"cf":"user","col":"lastAddressId","type":"string"},
       |"nationality":{"cf":"user","col":"nationality","type":"string"},
       |"marriage":{"cf":"user","col":"marriage","type":"string"},
       |"politicalFace":{"cf":"user","col":"politicalFace","type":"string"},
       |"qq":{"cf":"user","col":"qq","type":"string"},
       |"mobile":{"cf":"user","col":"mobile","type":"string"},
       |"source":{"cf":"user","col":"source","type":"string"}
       |}
       |}""".stripMargin

  //查询aft_basic_biz
  def catalog2 =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_log"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"browseFrequency":{"cf":"log", "col":"browseFrequency", "type":"string"}
       |}
       |}""".stripMargin


  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_comb"},
       |"rowkey":"id",
       |"columns":{
       |"user_id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"browseFrequency":{"cf":"oftenLeoFemale", "col":"browseFrequency", "type":"string"},
       |"ageGroup":{"cf":"oftenLeoFemale","col":"ageGroup","type":"string"},
       |"birthday":{"cf":"oftenLeoFemale","col":"birthday","type":"string"},
       |"constellation":{"cf":"oftenLeoFemale","col":"constellation","type":"string"},
       |"gender":{"cf":"oftenLeoFemale","col":"gender","type":"string"},
       |"job":{"cf":"oftenLeoFemale","col":"job","type":"string"},
       |"lastAddressId":{"cf":"oftenLeoFemale","col":"lastAddressId","type":"string"},
       |"nationality":{"cf":"oftenLeoFemale","col":"nationality","type":"string"},
       |"marriage":{"cf":"oftenLeoFemale","col":"marriage","type":"string"},
       |"politicalFace":{"cf":"oftenLeoFemale","col":"politicalFace","type":"string"},
       |"qq":{"cf":"oftenLeoFemale","col":"qq","type":"string"},
       |"mobile":{"cf":"oftenLeoFemale","col":"mobile","type":"string"},
       |"source":{"cf":"oftenLeoFemale","col":"source","type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("shc test")
    .master("local[10]")
    .getOrCreate()
  import spark.implicits._

  val userDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog1)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()
  val orderDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog2)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val joinExpression = userDF.col("user_id") === orderDF.col("id")

  val OftenLeoFemaleS = userDF
    .join(orderDF,joinExpression)
    .where('gender === "女")
    .where('constellation === "狮子座")
    .where('browseFrequency === "经常")

  val OftenLeoFemaleW = OftenLeoFemaleS
    .select('user_id,'browseFrequency,'ageGroup,'birthday,'constellation,'gender,'job,
      'lastAddressId,'nationality,'marriage, 'politicalFace,'qq,'mobile,'source)


  def OftenLeoFemaleWrite ={

    OftenLeoFemaleS
      .show()

    try{

      OftenLeoFemaleW.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("OftenLeoFemaleWrite finish")

    }


    spark.close()

  }


}
