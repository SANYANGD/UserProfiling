package net.suncaper.ten.basic.mining

import breeze.signal.OptRange.All
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object CateFeaturedModel {


  val spark = SparkSession.builder()
    .appName("GenderName")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  def ordersCatalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_orders"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
       |"relationOrderSn":{"cf":"cf", "col":"relationOrderSn", "type":"string"}
       |}
       |}""".stripMargin

  def goodsCatalog =
    s"""{
       |"table":{"namespace":"default", "name":"tbl_goods"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
       |"cateId":{"cf":"cf", "col":"cateId", "type":"string"}
       |}
       |}""".stripMargin

  def cateFeaturedCatalog =
    s"""{
       |  "table":{"namespace":"default", "name":"aft_basic_biz"},
       |  "rowkey":"id",
       |   "columns":{
       |     "id":{"cf":"rowkey", "col":"id", "type":"string"},
       |     "cateFeatured":{"cf":"biz", "col":"cateFeatured", "type":"string"}
       |   }
       |}""".stripMargin

  //val url2ProductId = udf(getProductId _)

  val ordersDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, ordersCatalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()



  val goodsDF: DataFrame = spark.read
    .option(HBaseTableCatalog.tableCatalog, goodsCatalog)
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()


  val joinExpression = ordersDF.col("relationOrderSn") === goodsDF.col("cOrderSn")

  val cateIdDF = ordersDF
    .join(goodsDF, joinExpression)


  val ratingDF = cateIdDF.select(
    'memberId.cast(DataTypes.IntegerType),
    'cateId.cast(DataTypes.IntegerType))
    .filter('cateId.isNotNull)
    .groupBy('memberId, 'cateId)
    .agg(count('cateId) as "rating")


  val als = new ALS()
    .setUserCol("memberId")
    .setItemCol("cateId")
    .setRatingCol("rating")
    .setPredictionCol("predict")
    .setColdStartStrategy("drop")
    .setAlpha(10)
    .setMaxIter(5)
    .setRank(5)
    .setRegParam(0.8)
    .setImplicitPrefs(true)

  val model: ALSModel = als.fit(ratingDF)


  //    // 将数据集切分为两份，其中训练集占80%(0.8), 测试集占20%(0.2)
  //    val Array(trainSet, testSet) = ratingDF.randomSplit(Array(0.8, 0.2))
  //
  //    // 回归模型评测器
  //    val evaluator: RegressionEvaluator = new RegressionEvaluator()
  //      .setLabelCol("rating")
  //      .setPredictionCol("predict")
  //      .setMetricName("rmse")
  //
  //    // 通过训练集进行训练，建立模型
  //    val model: ALSModel = als.fit(trainSet)
  //
  //    // 通过模型进行预测
  //    val predictions = model.transform(trainSet)
  //
  //    val rmse = evaluator.evaluate(predictions)
  //
  //
  //    //model.write.overwrite().save("model/product/als")
  //
  //    println(s"rmse value is ${rmse}")


  //val model = ALSModel.load("model/product/als")

  val predict2StringFunc = udf(predict2String _)


  // 为每个用户推荐
  val resultTemp: DataFrame = model.recommendForAllUsers(10)
    .withColumn("cateFeatured", predict2StringFunc('recommendations))
    .withColumnRenamed("memberId", "id")
    .drop('recommendations)




  //去重

  val resultTemp1 = resultTemp
    .withColumn("temp1", split(col("cateFeatured"), ","))
    .withColumn("temp2", explode(col("temp1")))
    .withColumnRenamed("temp2", "cateId")
    .withColumnRenamed("id", "memberId")
    .drop("temp1")
    .drop("cateFeatured")



  val resultTemp2 = cateIdDF
    .select("memberId","cateId")



  val resultTemp3 = resultTemp1
    .except(resultTemp2)
    .withColumnRenamed("cateId", "cateFeatured")
    .withColumnRenamed("memberId", "id")



  val cate = concat_ws(",",collect_set("cateFeatured")).as("cate")

  val result = resultTemp3
    .groupBy("id","cateFeatured")
    .agg(cate)
    .drop("cateFeatured")
    .withColumnRenamed("cate", "cateFeatured")



  def cateFeaturedModel(): Unit ={

    //设定spark的日志级别为warning，只是打印警告和错误信息
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    ordersDF.show()
    goodsDF.show()
    cateIdDF.show()
    println(cateIdDF.count())
    ratingDF.show()
    resultTemp.show()
    resultTemp1.show()
    resultTemp2.show()
    resultTemp3.show()
    result.show()
    //result = resultTemp.select('id.cast(DataTypes.StringType), 'cateFeatured)

    try {

      result.write
        .option(HBaseTableCatalog.tableCatalog, cateFeaturedCatalog)
        .option(HBaseTableCatalog.newTable, "5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()

    } catch {

      case ex: IllegalArgumentException =>

    } finally {

      println("cateFeaturedW finish")

    }

    spark.stop()

  }

  def predict2String(arr: Seq[Row]) = {
    arr.map(_.getAs[Int]("cateId")).mkString(",")
  }


  def main(args: Array[String]): Unit = {

    cateFeaturedModel()

  }


}













