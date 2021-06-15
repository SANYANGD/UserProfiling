package net.suncaper.ten.basic.matching

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

class LastAddressId {

  def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"lastAddressId":{"cf":"cf", "col":"lastAddressId", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_user"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"lastAddressId":{"cf":"user", "col":"lastAddressId", "type":"string"}
       |}
       |}""".stripMargin

  val spark = SparkSession.builder()
    .appName("shc test")
    .master("local[10]")
    .getOrCreate()

  val readDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()

  val lastAddressIdW = readDF

  def lastAddressIdWrite={
    readDF.show()
    lastAddressIdW.show()

        lastAddressIdW.write
          .option(HBaseTableCatalog.tableCatalog, catalogWrite)
          .option(HBaseTableCatalog.newTable, "5")
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()

    spark.close()
  }

}
