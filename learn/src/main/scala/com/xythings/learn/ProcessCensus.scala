package com.xythings.learn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

/**
  * Created by tim on 05/09/2017.
  */
object ProcessCensus {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("GBTLoader")
    .master("local")
    .getOrCreate()

  val context = spark.sqlContext

  def main(args: Array[String]): Unit = {

    val train = true
    val validate = true

    val trainFileDF = spark.read.parquet("data/splits/key=1")
    val validationFileDF = spark.read.parquet("data/splits/key=2")
    val testFileDF = spark.read.parquet("data/splits/key=3")

    if (train){
      // Train a RandomForest model.
      val rf = new GBTClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("normFeatures")

      println(rf.getLossType)
      println(rf.getMinInfoGain)
      println(rf.getImpurity)
      println(rf.getSubsamplingRate)
      println(rf.getStepSize)

      val stages = Array(rf)

      val pipeline = new Pipeline()
        .setStages(stages)

      /**
        * These are the learning grid values I arrived at, add other values to verify them as the best model
        */
      val paramMaps = new ParamGridBuilder()
        .addGrid(rf.maxIter, Array(120))
        .addGrid(rf.maxDepth, Array(5))
        .addGrid(rf.maxBins, Array(83))
        .addGrid(rf.stepSize, Array(.21))
        .addGrid(rf.impurity, Array("gini"))
        .addGrid(rf.minInfoGain, Array(0.0001))
        .build()

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("f1")

      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(evaluator)
        .setNumFolds(3)
        .setEstimatorParamMaps(paramMaps)

      val cvModel = cv.fit(trainFileDF)

      val cvWriter = cvModel.write.overwrite()
      cvWriter.save("data/ml/gbt_split_f2")
    }

    if (validate){

      val sameCVModel = CrossValidatorModel.load("data/ml/gbt_split_f2")

      // Make predictions.
      val predictions = sameCVModel.bestModel.transform(validationFileDF)

      // Select (prediction, true label) and compute test error.

      val accuracy = sameCVModel.getEvaluator.evaluate(predictions)

      predictions.select("prediction", "label", "indexedLabel", "indexedFeatures").filter(_.get(2).equals(1.0)).show(10)

      val matched = predictions.select("prediction", "indexedLabel").filter(col => {
        col.get(1).equals(col.get(0))
      })

      val matchCount = matched.count()
      val predictionsCount = predictions.count()
      val matching = matchCount.toDouble/predictionsCount.toDouble

      println(s"Test Error = ${(1.0 - accuracy)}, accuracy = $accuracy")

      println(matchCount, predictionsCount, (predictionsCount-matchCount), matching * 100)

      val castModel = sameCVModel.bestModel.asInstanceOf[PipelineModel]

      println(castModel.stages(0).explainParams())

    }
  }
}

