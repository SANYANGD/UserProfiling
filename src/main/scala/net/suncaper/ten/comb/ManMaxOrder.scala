package net.suncaper.ten.comb

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

class ManMaxOrder {
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
       |"username":{"cf":"user","col":"username","type":"string"},
       |"ageGroup":{"cf":"user","col":"ageGroup","type":"string"},
       |"birthday":{"cf":"user","col":"birthday","type":"string"},
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
       |"username":{"cf":"80sManMaxOrder","col":"username","type":"string"},
       |"maxOrderAmount":{"cf":"80sManMaxOrder", "col":"maxOrderAmount", "type":"string"},
       |"ageGroup":{"cf":"80sManMaxOrder","col":"ageGroup","type":"string"},
       |"birthday":{"cf":"80sManMaxOrder","col":"birthday","type":"string"},
       |"gender":{"cf":"80sManMaxOrder","col":"gender","type":"string"},
       |"job":{"cf":"80sManMaxOrder","col":"job","type":"string"},
       |"lastAddressId":{"cf":"80sManMaxOrder","col":"lastAddressId","type":"string"},
       |"nationality":{"cf":"80sManMaxOrder","col":"nationality","type":"string"},
       |"marriage":{"cf":"80sManMaxOrder","col":"marriage","type":"string"},
       |"politicalFace":{"cf":"80sManMaxOrder","col":"politicalFace","type":"string"},
       |"qq":{"cf":"80sManMaxOrder","col":"qq","type":"string"},
       |"mobile":{"cf":"80sManMaxOrder","col":"mobile","type":"string"},
       |"source":{"cf":"80sManMaxOrder","col":"source","type":"string"}
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

  val ManMaxOrderS = userDF
    .join(orderDF,joinExpression)
    .where('gender === "男")
    .where('ageGroup === "80后")
    .where('maxOrderAmount === "最高：5000-9999")

  val ManMaxOrderW = ManMaxOrderS
      .select('user_id,'username,'maxOrderAmount,'ageGroup,'birthday,'gender,'job,
        'lastAddressId,'nationality,'marriage, 'politicalFace,'qq,'mobile,'source)


  def ManMaxOrderWrite ={

    ManMaxOrderS.show()

    try{

      ManMaxOrderW.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("ManMaxOrderWrite finish")

    }


    spark.close()

  }


}
