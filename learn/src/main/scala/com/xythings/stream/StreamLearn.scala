package com.xythings.stream

/**
  * Created by tim on 05/09/2017.
  */
import java.sql.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, GenericInternalRow, Literal}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object StreamLearn extends App {

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("StreamLearn")
    .master("local")
    .getOrCreate()

  spark.conf.getAll.foreach(pair => {
    println(pair._1, pair._2)
  })

  import spark.implicits._

  def infer(e: org.apache.spark.sql.Column, fields: String*): org.apache.spark.sql.Column = new org.apache.spark.sql.Column(
    Inference(e.expr +: fields.map(Literal.apply))
  )

  //If we only want the latest offsets uncomment "startingOffsets" line
  val df = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "595a62a1641d21dedbfcb365")
    //.option("startingOffsets", "earliest")
    .load()

  df.printSchema()

  val sumSelect = df.select(
    ($"key").cast("string"),
    infer(($"value").cast("string"), "value").as(Seq("inferred", "matched", "time")),
    ($"offset").cast("double")
  )

  val sumCast = sumSelect.withColumn("times", sumSelect.col("time").cast(TimestampType))
    .as[(String, Double, Double, Date, Double, Timestamp)]
    .groupBy($"key", window($"time", "4 minutes"))
    .agg(sum("inferred"), sum("matched"), count("offset"))
    .orderBy(desc("window"))

  var sumQuery = sumCast.writeStream
    .queryName("summing")    // this query name will be the table name
    .outputMode("complete")
    .format("memory")

  val selection = df.select(
    ($"key").cast("string"),
    json_tuple(($"value").cast("string"), "time").as("time"),
    ($"value").cast("string"),
    ($"offset").cast("double"),
    ($"timestamp").cast("timestamp"))

  val recast = selection.withColumn("times", selection.col("time").cast(TimestampType))
    .as[(String, String, String, Double, Timestamp, Timestamp)]

  val streamSelection = recast
    .distinct()

  spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
      println("Query started: " + queryStarted.id)
    }
    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      println("Query terminated: " + queryTerminated.id)
    }
    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      if (queryProgress.progress.numInputRows > 0){
        spark.sql(s"select * from ${queryProgress.progress.name}").show(30)
      }
    }
  })

  val summing = sumQuery.start()

  println(summing.id, summing.name)

  try {
    summing.awaitTermination
  }
  catch {
    case e: Exception =>
      e.printStackTrace()
  }
  finally {
    println(s"summing no longer active: terminating $summing")

    spark.stop()
  }

}

case class ActionTime(val action: CensusData, val time: String)

/**
root
 |-- age: integer (nullable = true)
 |-- workclass: string (nullable = true)
 |-- fnlwgt: double (nullable = true)
 |-- education: string (nullable = true)
 |-- education-num: double (nullable = true)
 |-- marital-status: string (nullable = true)
 |-- occupation: string (nullable = true)
 |-- relationship: string (nullable = true)
 |-- race: string (nullable = true)
 |-- sex: string (nullable = true)
 |-- capital-gain: double (nullable = true)
 |-- capital-loss: double (nullable = true)
 |-- hours-per-week: double (nullable = true)
 |-- native-country: string (nullable = true)
 |-- label: string (nullable = true)
  */
case class CensusData(age: Integer, workclass: String, fnlwgt: Double, education: String, `education-num`: Integer,
                      `marital-status`: String, occupation: String, relationship: String, race: String, sex: String, `capital-gain`: Double,
                      `capital-loss`: Double, `hours-per-week`: Double, `native-country`: String, label: String)

case class Inference(children: Seq[Expression]) extends Generator with CodegenFallback {

  val dataModel = PipelineModel.load("data/ml/gbtdata")

  val sameCVModel = CrossValidatorModel.load("data/ml/gbt")

  val sqlFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  // if processing fails this shared value will be returned
  @transient private lazy val nullRow: Seq[InternalRow] = new GenericInternalRow(Array.ofDim[Any](3)) :: Nil

  @transient private lazy val jsonExpr: Expression = children.head

  @transient private lazy val fieldExpressions: Seq[Expression] = children.tail

  @transient private lazy val spark = SparkSession
    .builder
    .appName("StreamLearn")
    .master("local")
    .getOrCreate()

  override def elementSchema: StructType = StructType(
    Seq(
      StructField("inferred", DoubleType, nullable = true),
      StructField("matched", DoubleType, nullable = true),
      StructField("time", StringType, nullable = true)
    )
  )

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {

    print(".")

    implicit val formats = DefaultFormats
    val jsonString = jsonExpr.eval(input).asInstanceOf[UTF8String]

    try {
      val json = parse(jsonString.toString).camelizeKeys

      val at = json.extract[ActionTime]

      import spark.implicits._

      val caseClassDS = Seq(at.action).toDF()

      val colNames = caseClassDS.schema.toList.map(field => field.name.replaceAll("[$]minus", "-"))
      val dfRenamed = caseClassDS.toDF(colNames: _*)

      val transformedAction = dataModel.transform(dfRenamed)

      val inferrence = sameCVModel.transform(transformedAction)

      val results = inferrence.first().getValuesMap(Seq("label", "indexedLabel", "prediction", "predictedLabel"))

      val inferredValue = results("prediction").asInstanceOf[Double]
      val label = results("label").asInstanceOf[String]
      val predictedLabel = results("predictedLabel").asInstanceOf[String]

      val matching = if (label.equals(predictedLabel)) 1.0 else 0.0

      val date = isoFormat.parse(at.time)

      val sqlTime = sqlFormat.format(date)

      val row = Array.ofDim[Any](3)

      row(0) = inferredValue
      row(1) = matching
      row(2) = UTF8String.fromString(sqlTime)

      val internalRow = new GenericInternalRow(row)

      internalRow :: Nil
    }
    catch {
      case e: Exception =>
        e.printStackTrace
        nullRow
    }

  }
}
