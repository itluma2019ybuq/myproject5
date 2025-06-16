import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

object ProductionOptimization {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ProductionOptimization")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    println("=== 生产工艺优化系统启动 ===")
    
    // 子任务一：特征工程
    val featureEngineering = new FeatureEngineering(spark)
    val processedData = featureEngineering.processData()
    
    // 子任务二：参数优化
    val parameterOptimization = new ParameterOptimization(spark)
    parameterOptimization.optimizeParameters(processedData)
    
    spark.stop()
  }
}

class FeatureEngineering(spark: SparkSession) {
  import spark.implicits._
  
  def processData(): DataFrame = {
    println("\n=== 子任务一：特征工程 ===")
    
    // 1. 解析生产设备日志提取工艺参数
    val productionLogs = loadProductionLogs()
    println("1. 生产设备日志加载完成")
    productionLogs.show(10)
    
    // 2. 关联质量检测结果数据
    val qualityResults = loadQualityResults()
    println("2. 质量检测结果加载完成")
    qualityResults.show(10)
    
    // 3. 处理生产过程中的缺失值
    val cleanedData = processMissingValues(productionLogs)
    println("3. 缺失值处理完成")
    
    // 4. 关联数据
    val joinedData = joinData(cleanedData, qualityResults)
    println("4. 数据关联完成")
    joinedData.show(10)
    
    // 5. 特征统计
    printFeatureStatistics(joinedData)
    
    joinedData
  }
  
  private def loadProductionLogs(): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/production_logs.csv")
      .withColumn("timestamp", to_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss"))
  }
  
  private def loadQualityResults(): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/quality_results.csv")
  }
  
  private def processMissingValues(df: DataFrame): DataFrame = {
    println("处理缺失值统计：")
    df.columns.foreach { col =>
      val missingCount = df.filter(df(col).isNull).count()
      val totalCount = df.count()
      val missingRate = missingCount.toDouble / totalCount * 100
      println(s"  $col: ${missingCount}/${totalCount} (${f"$missingRate%.2f"}%)")
    }
    
    // 使用均值填充数值型缺失值
    val numericCols = Array("temperature", "pressure", "rotation_speed", "flow_rate", "ph_value")
    
    val meanValues = numericCols.map { colName =>
      val meanVal = df.select(mean(col(colName))).collect()(0).getDouble(0)
      (colName, meanVal)
    }.toMap
    
    var cleanedDf = df
    meanValues.foreach { case (colName, meanVal) =>
      cleanedDf = cleanedDf.na.fill(meanVal, Seq(colName))
    }
    
    println("缺失值填充完成")
    cleanedDf
  }
  
  private def joinData(productionLogs: DataFrame, qualityResults: DataFrame): DataFrame = {
    // 按批次聚合生产参数
    val aggregatedLogs = productionLogs.groupBy("batch_id")
      .agg(
        avg("temperature").alias("avg_temperature"),
        avg("pressure").alias("avg_pressure"),
        avg("rotation_speed").alias("avg_rotation_speed"),
        avg("flow_rate").alias("avg_flow_rate"),
        avg("ph_value").alias("avg_ph_value"),
        stddev("temperature").alias("std_temperature"),
        stddev("pressure").alias("std_pressure"),
        count("*").alias("record_count")
      )
    
    // 关联质量数据
    aggregatedLogs.join(qualityResults, "batch_id")
  }
  
  private def printFeatureStatistics(df: DataFrame): Unit = {
    println("\n特征统计信息：")
    df.describe().show()
    
    println("数据样本数量：" + df.count())
    println("特征维度：" + df.columns.length)
  }
}

class ParameterOptimization(spark: SparkSession) {
  import spark.implicits._
  
  def optimizeParameters(data: DataFrame): Unit = {
    println("\n=== 子任务二：参数优化 ===")
    
    // 1. 建立质量预测回归模型
    val model = buildPredictionModel(data)
    println("1. 质量预测模型构建完成")
    
    // 2. 使用遗传算法寻找最优工艺参数组合
    val optimalParams = geneticAlgorithmOptimization(model, data)
    println("2. 遗传算法优化完成")
    
    // 3. 输出参数优化建议
    outputOptimizationSuggestions(optimalParams, data)
    println("3. 参数优化建议输出完成")
  }
  
  private def buildPredictionModel(data: DataFrame): PipelineModel = {
    // 准备特征向量
    val featureCols = Array("avg_temperature", "avg_pressure", "avg_rotation_speed", 
                           "avg_flow_rate", "avg_ph_value", "std_temperature", "std_pressure")
    
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    
    val rf = new RandomForestRegressor()
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("quality_score")
      .setNumTrees(50)
      .setMaxDepth(10)
    
    val pipeline = new Pipeline()
      .setStages(Array(assembler, scaler, rf))
    
    // 训练模型
    val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 42)
    val model = pipeline.fit(trainData)
    
    // 评估模型
    val predictions = model.transform(testData)
    val evaluator = new RegressionEvaluator()
      .setLabelCol("quality_score")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    
    val rmse = evaluator.evaluate(predictions)
    println(s"模型RMSE: ${f"$rmse%.3f"}")
    
    // 显示预测结果示例
    predictions.select("quality_score", "prediction", "avg_temperature", "avg_pressure")
      .show(10)
    
    model
  }
  
  private def geneticAlgorithmOptimization(model: PipelineModel, data: DataFrame): Map[String, Double] = {
    println("开始遗传算法优化...")
    
    // 参数范围定义
    val paramRanges = Map(
      "avg_temperature" -> (80.0, 100.0),
      "avg_pressure" -> (2.0, 4.0),
      "avg_rotation_speed" -> (1100.0, 1400.0),
      "avg_flow_rate" -> (40.0, 60.0),
      "avg_ph_value" -> (6.5, 8.5)
    )
    
    val populationSize = 50
    val generations = 20
    val mutationRate = 0.1
    val eliteSize = 5
    
    // 初始化种群
    var population = initializePopulation(populationSize, paramRanges)
    
    for (generation <- 1 to generations) {
      // 评估适应度
      val fitness = population.map(individual => evaluateFitness(individual, model))
      
      // 选择、交叉、变异
      population = evolvePopulation(population, fitness, paramRanges, eliteSize, mutationRate)
      
      val bestFitness = fitness.max
      if (generation % 5 == 0) {
        println(s"第${generation}代 - 最佳适应度: ${f"$bestFitness%.3f"}")
      }
    }
    
    // 返回最优参数
    val finalFitness = population.map(individual => evaluateFitness(individual, model))
    val bestIndex = finalFitness.zipWithIndex.maxBy(_._1)._2
    population(bestIndex)
  }
  
  private def initializePopulation(size: Int, ranges: Map[String, (Double, Double)]): Array[Map[String, Double]] = {
    val random = new Random()
    (0 until size).map { _ =>
      ranges.map { case (param, (min, max)) =>
        param -> (random.nextDouble() * (max - min) + min)
      }
    }.toArray
  }
  
  private def evaluateFitness(individual: Map[String, Double], model: PipelineModel): Double = {
    // 创建预测数据
    val spark = SparkSession.active
    import spark.implicits._
    
    val testData = Seq(
      (individual("avg_temperature"), individual("avg_pressure"), individual("avg_rotation_speed"),
       individual("avg_flow_rate"), individual("avg_ph_value"), 1.0, 0.5)
    ).toDF("avg_temperature", "avg_pressure", "avg_rotation_speed", 
           "avg_flow_rate", "avg_ph_value", "std_temperature", "std_pressure")
    
    val prediction = model.transform(testData)
    prediction.select("prediction").collect()(0).getDouble(0)
  }
  
  private def evolvePopulation(population: Array[Map[String, Double]], 
                              fitness: Array[Double],
                              ranges: Map[String, (Double, Double)],
                              eliteSize: Int,
                              mutationRate: Double): Array[Map[String, Double]] = {
    val random = new Random()
    val newPopulation = ArrayBuffer[Map[String, Double]]()
    
    // 精英保留
    val sortedIndices = fitness.zipWithIndex.sortBy(-_._1).map(_._2)
    for (i <- 0 until eliteSize) {
      newPopulation += population(sortedIndices(i))
    }
    
    // 生成新个体
    while (newPopulation.length < population.length) {
      // 选择父母（轮盘赌选择）
      val parent1 = selectParent(population, fitness, random)
      val parent2 = selectParent(population, fitness, random)
      
      // 交叉
      val child = crossover(parent1, parent2, random)
      
      // 变异
      val mutatedChild = mutate(child, ranges, mutationRate, random)
      
      newPopulation += mutatedChild
    }
    
    newPopulation.toArray
  }
  
  private def selectParent(population: Array[Map[String, Double]], 
                          fitness: Array[Double], 
                          random: Random): Map[String, Double] = {
    val totalFitness = fitness.sum
    val r = random.nextDouble() * totalFitness
    var cumulative = 0.0
    
    for (i <- fitness.indices) {
      cumulative += fitness(i)
      if (cumulative >= r) {
        return population(i)
      }
    }
    population.last
  }
  
  private def crossover(parent1: Map[String, Double], 
                       parent2: Map[String, Double], 
                       random: Random): Map[String, Double] = {
    parent1.map { case (param, value1) =>
      val value2 = parent2(param)
      val alpha = random.nextDouble()
      param -> (alpha * value1 + (1 - alpha) * value2)
    }
  }
  
  private def mutate(individual: Map[String, Double], 
                    ranges: Map[String, (Double, Double)], 
                    mutationRate: Double,
                    random: Random): Map[String, Double] = {
    individual.map { case (param, value) =>
      if (random.nextDouble() < mutationRate) {
        val (min, max) = ranges(param)
        val newValue = value + random.nextGaussian() * (max - min) * 0.1
        param -> math.max(min, math.min(max, newValue))
      } else {
        param -> value
      }
    }
  }
  
  private def outputOptimizationSuggestions(optimalParams: Map[String, Double], 
                                          originalData: DataFrame): Unit = {
    println("\n=== 参数优化建议 ===")
    
    // 计算原始数据的平均值
    val originalMeans = originalData.select(
      avg("avg_temperature").alias("orig_temp"),
      avg("avg_pressure").alias("orig_pressure"),
      avg("avg_rotation_speed").alias("orig_speed"),
      avg("avg_flow_rate").alias("orig_flow"),
      avg("avg_ph_value").alias("orig_ph")
    ).collect()(0)
    
    println("最优工艺参数组合：")
    println(f"温度: ${optimalParams("avg_temperature")}%.2f°C (原始平均: ${originalMeans.getDouble(0)}%.2f°C)")
    println(f"压力: ${optimalParams("avg_pressure")}%.2f MPa (原始平均: ${originalMeans.getDouble(1)}%.2f MPa)")
    println(f"转速: ${optimalParams("avg_rotation_speed")}%.0f rpm (原始平均: ${originalMeans.getDouble(2)}%.0f rpm)")
    println(f"流量: ${optimalParams("avg_flow_rate")}%.2f L/min (原始平均: ${originalMeans.getDouble(3)}%.2f L/min)")
    println(f"pH值: ${optimalParams("avg_ph_value")}%.2f (原始平均: ${originalMeans.getDouble(4)}%.2f)")
    
    // 计算改进建议
    println("\n改进建议：")
    val tempDiff = optimalParams("avg_temperature") - originalMeans.getDouble(0)
    val pressureDiff = optimalParams("avg_pressure") - originalMeans.getDouble(1)
    val speedDiff = optimalParams("avg_rotation_speed") - originalMeans.getDouble(2)
    val flowDiff = optimalParams("avg_flow_rate") - originalMeans.getDouble(3)
    val phDiff = optimalParams("avg_ph_value") - originalMeans.getDouble(4)
    
    if (math.abs(tempDiff) > 1.0) {
      println(f"• 温度${if (tempDiff > 0) "提高" else "降低"}${math.abs(tempDiff)}%.2f°C")
    }
    if (math.abs(pressureDiff) > 0.1) {
      println(f"• 压力${if (pressureDiff > 0) "提高" else "降低"}${math.abs(pressureDiff)}%.2f MPa")
    }
    if (math.abs(speedDiff) > 10) {
      println(f"• 转速${if (speedDiff > 0) "提高" else "降低"}${math.abs(speedDiff)}%.0f rpm")
    }
    if (math.abs(flowDiff) > 1.0) {
      println(f"• 流量${if (flowDiff > 0) "提高" else "降低"}${math.abs(flowDiff)}%.2f L/min")
    }
    if (math.abs(phDiff) > 0.1) {
      println(f"• pH值${if (phDiff > 0) "提高" else "降低"}${math.abs(phDiff)}%.2f")
    }
  }
} 