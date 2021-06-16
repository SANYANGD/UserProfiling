package net.suncaper.ten.comb

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

class PolicemanMaxOrder {
  //单笔最高10000以上的男警察
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
       |"gender":{"cf":"user","col":"gender","type":"string"},
       |"job":{"cf":"user","col":"job","type":"string"},
       |"lastAddressId":{"cf":"user","col":"lastAddressId","type":"string"},
       |"nationality":{"cf":"user","col":"nationality","type":"string"},
       |"marriage":{"cf":"user","col":"marriage","type":"string"},
       |"politicalFace":{"cf":"user","col":"politicalFace","type":"string"},
       |"qq":{"cf":"user","col":"qq","type":"string"},
       |"source":{"cf":"user","col":"source","type":"string"}
       |}
       |}""".stripMargin

  //查询aft_basic_biz
  def catalog2 =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_biz"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"maxOrderAmount":{"cf":"biz", "col":"maxOrderAmount", "type":"string"}
       |}
       |}""".stripMargin


  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_comb"},
       |"rowkey":"id",
       |"columns":{
       |"user_id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"maxOrderAmount":{"cf":"comb", "col":"maxOrderAmount", "type":"string"},
       |"ageGroup":{"cf":"comb","col":"ageGroup","type":"string"},
       |"birthday":{"cf":"comb","col":"birthday","type":"string"},
       |"gender":{"cf":"comb","col":"gender","type":"string"},
       |"job":{"cf":"comb","col":"job","type":"string"},
       |"lastAddressId":{"cf":"comb","col":"lastAddressId","type":"string"},
       |"nationality":{"cf":"comb","col":"nationality","type":"string"},
       |"marriage":{"cf":"comb","col":"marriage","type":"string"},
       |"politicalFace":{"cf":"comb","col":"politicalFace","type":"string"},
       |"qq":{"cf":"comb","col":"qq","type":"string"},
       |"source":{"cf":"comb","col":"source","type":"string"}
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

  val goodsBoughtS = userDF
    .join(orderDF,joinExpression)
    .where('gender === "男")
    .where('job === "警察")
    .where('maxOrderAmount === "10000-")

  val goodsBoughtW = goodsBoughtS
      .select('user_id,'maxOrderAmount,'ageGroup,'birthday,'gender,'job,
        'lastAddressId,'nationality,'marriage, 'politicalFace,'qq,'source)

  goodsBoughtS
    .show()

  goodsBoughtW.write
    .option(HBaseTableCatalog.tableCatalog, catalogWrite)
    .option(HBaseTableCatalog.newTable, "5")
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .save()


  spark.close()
}
