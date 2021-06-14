package net.suncaper.ten.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

object genderTest {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    println("1")

    readDF.show()

//    def catalogWrite =
//      s"""{
//         |"table":{"namespace":"default", "name":"dcy_test"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
//         |"gender":{"cf":"cf", "col":"gender", "type":"string"}
//         |}
//         |}""".stripMargin
//
//    readDF.write
//      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
  }

}
