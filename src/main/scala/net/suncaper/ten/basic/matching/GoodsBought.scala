package net.suncaper.ten.basic.matching

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

class GoodsBought {

  //查询tbl_users
  def catalog1 =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"users_id":{"cf":"rowkey","col":"id", "type":"string"},
       |"username":{"cf":"cf","col":"username","type":"string"}
       |}
       |}""".stripMargin

  //查询tbl_orders
  def catalog2 =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_orders"},
       |"rowkey":"id",
       |"columns":{
       |"orders_id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
       |"memberId":{"cf":"cf", "col":"memberId", "type":"string"}
       |}
       |}""".stripMargin

  //查询tbl_goods
  def catalog3 =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_goods"},
       |"rowkey":"id",
       |"columns":{
       |"goods_id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
       |"productId":{"cf":"cf", "col":"productId", "type":"string"},
       |"productName":{"cf":"cf", "col":"productName", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_beh"},
       |"rowkey":"id",
       |"columns":{
       |"users_id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"username":{"cf":"behavior","col":"username","type":"string"},
       |"productName":{"cf":"behavior", "col":"goodsBought", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("shc test")
    .master("local[10]")
    .getOrCreate()

  val userDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog1)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()
  val orderDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog2)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()
  val goodsDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog3)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val joinExpression = userDF.col("users_id") === orderDF.col("memberId")
  val joinExpression2 = orderDF.col("orderSn") === goodsDF.col("cOrderSn")


  val goodsBoughtS = userDF
            .join(orderDF,joinExpression)
            .join(goodsDF,joinExpression2)


  val goodsBoughtW = goodsBoughtS
    .select("users_id","username","productName")


  def goodsBoughtWrite={

    goodsBoughtS.show()


    try{

      goodsBoughtW.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("goodsBoughtW finish")

    }



    spark.close()

  }

}
