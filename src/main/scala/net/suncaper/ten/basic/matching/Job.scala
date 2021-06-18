package net.suncaper.ten.basic.matching

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Job {

  def catalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_users"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"job":{"cf":"cf", "col":"job", "type":"string"}
       |}
       |}""".stripMargin

  def catalogWrite =
    s"""{
       |"table":{"namespace":"default", "name":"aft_basic_user"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"job":{"cf":"user", "col":"job", "type":"string"}
       |}
       |}""".stripMargin

  def finalWrite =
    s"""{
       |"table":{"namespace":"default", "name":"final"},
       |"rowkey":"job",
       |"columns":{
       |"job":{"cf":"rowkey", "col":"job", "type":"string"},
       |"number":{"cf":"cf", "col":"val", "type":"string"}
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

  //职业；1学生、2公务员、3军人、4警察、5教师、6白领
  val jobW = readDF.select('id,
    when('job === "1", "学生")
      .when('job === "2", "公务员")
      .when('job === "3", "军人")
      .when('job === "4", "警察")
      .when('job === "5", "教师")
      .when('job === "6", "白领")
      .otherwise("其他")
      .as("job")
  )
  val finalJobW = jobW
    .select('id,'job)
    .groupBy('job)
    .count()
    .withColumn("number",format_number('count,0))
    .drop('count)


  def jobWrite={
    readDF.show()
    jobW.show()
    finalJobW.show()

    try{

      jobW.write
        .option(HBaseTableCatalog.tableCatalog, catalogWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

      finalJobW.write
        .option(HBaseTableCatalog.tableCatalog, finalWrite)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    }catch {

      case ex: IllegalArgumentException =>

    }finally{

      println("jobWrite finish")

    }


    spark.close()
  }

}
