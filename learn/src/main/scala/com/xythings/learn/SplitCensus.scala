package com.xythings.learn

/**
  * Created by tim on 05/09/2017.
  */
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

object SplitCensus {

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("SplitCensus")
    .master("local")
    .getOrCreate()

  val context = spark.sqlContext

  def main(args: Array[String]): Unit = {

    // Load and parse the data file, converting it to a DataFrame.
    val data = spark.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/census.csv")

    println(data.schema)
    println(data.first())

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    labelIndexer.labels.foreach(a => println(a.toString))
    println

    /**
      * Define and Identify the Categorical variables
      * - our Gradient Boosted Tree can deal with categorical variable but this lets us use the same pipeline for other
      * learners
      */
    val categoricalVariables = Array("workclass", "education", "marital-status", "occupation", "relationship", "race", "sex")

    /**
      * Initialize the Categorical Variables as first state of the pipeline
      */
    val categoricalIndexers: Array[org.apache.spark.ml.PipelineStage] =
      categoricalVariables.map(i => new StringIndexer()
        .setInputCol(i).setOutputCol(i + "Index"))

    /**
      * Initialize the OneHotEncoder as another pipeline stage
      */
    val categoricalEncoders: Array[org.apache.spark.ml.PipelineStage] =
      categoricalVariables.map(e => new OneHotEncoder()
        .setInputCol(e + "Index").setOutputCol(e + "Vec"))

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "age", "education-num", "capital-gain", "capital-loss", "hours-per-week",
        "workclassVec", "educationVec", "marital-statusVec", "occupationVec",
        "relationshipVec", "raceVec", "sexVec"))
      .setOutputCol("features")

    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)

    val normalizer = new Normalizer()
      .setInputCol("indexedFeatures")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val dataStages = categoricalIndexers ++
      categoricalEncoders ++ Array(assembler, labelIndexer, featureIndexer, normalizer)

    val dataPipeline = new Pipeline()
      .setStages(dataStages)

    val dataModel = dataPipeline.fit(data)

    dataModel.write.overwrite.save("data/ml/gbtdata")

    val output = dataModel.transform(data)

    output.show()

    //Create a train/validate/test split
    val Array(trainingData, validation, testData) = output.randomSplit(Array(0.64, 0.16, 0.2))

    trainingData.write.parquet("data/splits/key=1")
    validation.write.parquet("data/splits/key=2")
    testData.write.parquet("data/splits/key=3")

  }
}