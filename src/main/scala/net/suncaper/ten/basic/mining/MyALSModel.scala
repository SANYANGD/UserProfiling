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

class MyALSModel {

  def myALSModel(): Unit = {


    //设定spark的日志级别为warning，只是打印警告和错误信息
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)


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

    //val url2ProductId = udf(getProductId _)

    val ordersDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, ordersCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //ordersDF.createTempView("ordersDF")

    //ordersDF.show()

    val goodsDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, goodsCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //goodsDF.createTempView("goodsDF")

    //goodsDF.show()

    val joinExpression = ordersDF.col("relationOrderSn") === goodsDF.col("cOrderSn")

    val cateIdDF = ordersDF
      .join(goodsDF, joinExpression)


    //cateIdDF.show()
    //println(cateIdDF.count())


    val ratingDF = cateIdDF.select(
      'memberId.cast(DataTypes.IntegerType),
      'cateId.cast(DataTypes.IntegerType))
      .filter('cateId.isNotNull)
      .groupBy('memberId, 'cateId)
      .agg(count('cateId) as "rating")

    //ratingDF.show()

    val als = new ALS()
      .setUserCol("memberId")
      .setItemCol("cateId")
      .setRatingCol("rating")
      .setPredictionCol("predict")
      .setColdStartStrategy("drop")
      .setAlpha(20)
      .setMaxIter(5)
      .setRank(5)
      .setRegParam(0.8)
      .setImplicitPrefs(true)

    //val model: ALSModel = als.fit(ratingDF)



    // 将数据集切分为两份，其中训练集占80%(0.8), 测试集占20%(0.2)
    val Array(trainSet, testSet) = ratingDF.randomSplit(Array(0.8, 0.2))

    // 回归模型评测器
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("predict")
      .setMetricName("rmse")

    // 通过训练集进行训练，建立模型
    val model: ALSModel = als.fit(trainSet)

    // 通过模型进行预测
    val predictions = model.transform(trainSet)

    val rmse = evaluator.evaluate(predictions)

    //model.write.overwrite().save("./alsmodel")
    model.write.overwrite().save("model/product/als")

    println(s"rmse value is ${rmse}")

    /*
    // 将数据集切分为两份，其中训练集占80%(0.8), 测试集占20%(0.2)
    val Array(trainSet, testSet) = ratingDF.randomSplit(Array(0.8, 0.2))

    // 回归模型评测器
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("predict")
      .setMetricName("rmse")

    // 通过训练集进行训练，建立模型
    val model: ALSModel = als.fit(trainSet)

    // 通过模型进行预测
    val predictions = model.transform(trainSet)

    val rmse = evaluator.evaluate(predictions)

    println(s"rmse value is ${rmse}")


    // 创建als pipeline
    val pipeline: Pipeline = new Pipeline().setStages(Array(als))

    // 构建参数网格
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(als.rank, Array[Int](5, 8, 10, 14, 17))
      .addGrid(als.maxIter, Array[Int](5, 7, 10, 13))
      .addGrid(als.regParam, Array[Double](0.01, 0.05, 0.1, 0.3, 0.5, 0.8))
      .build

    // 使用穷举搜索出最佳的参数
    val crossValidator: CrossValidator = new CrossValidator()
      .setEstimator(pipeline) // pipeline模型
      .setEvaluator(evaluator) // 评估器
      .setEstimatorParamMaps(paramGrid) // 参数网格
      .setNumFolds(2) // 现实中使用3+以上

    // 运行交叉检验，自动选择最佳的参数组合
    val crossValidatorModel: CrossValidatorModel = crossValidator.fit(trainSet)

    // 每个参数网格的平均指标
    val avgMetrics: Array[Double] = crossValidatorModel.avgMetrics

    // 打印所有的参数组合
    val estimatorParamMaps: Array[ParamMap] = crossValidatorModel.getEstimatorParamMaps
    for (i <- 1 to estimatorParamMaps.length - 1) {
      println(s"------ 第${i}次网格搜索的Metric：${avgMetrics(i)} -------")
      val paramMap: ParamMap = estimatorParamMaps(i)
      paramMap.toSeq.foreach(p => {
        println(s"----- 第${i}次网格搜索所选择训练模型的参数组合：${p.param.name} -> ${p.value}")
      })
    }

    // 最优的参数
    val bestPipeline: Pipeline = crossValidatorModel.bestModel.parent.asInstanceOf[Pipeline]
    val stage = bestPipeline.getStages(0)
    val paramMap: ParamMap = stage.extractParamMap
    val rank: Any = paramMap.get(stage.getParam("rank")).get
    val maxIter: Any = paramMap.get(stage.getParam("maxIter")).get
    val regParam: Any = paramMap.get(stage.getParam("regParam")).get
    println("==== ALS参数为：rank={},maxIter={},regParam={} ====", rank, maxIter, regParam)

    // 预测测试集
    val testPredicts: Dataset[Row] = crossValidatorModel.transform(testSet)
    // 评估测试集
    val testRmse: Double = evaluator.evaluate(testPredicts)
    // 预测训练集
    val trainPredicts: Dataset[Row] = crossValidatorModel.transform(trainSet)
    // 评估训练集
    val trainRmse: Double = evaluator.evaluate(trainPredicts)
    println("==== 本次推荐数据集和RMSE指标评估：训练集RMSE={},测试集RMSE={} ====", trainRmse, testRmse)
*/



  }

}
