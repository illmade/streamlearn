package com.xythings.stream

import org.apache.spark.sql.SparkSession
import org.http4s._
import org.http4s.dsl._
import org.http4s.client._
import org.http4s.client.blaze.{defaultClient => client}

import scala.util.parsing.json.JSONObject

/**
  * Created by tim on 05/09/2017.
  *
  * We have a nodeJs app that acts as our message interface - obviously we can just use a Kafka client to
  * connect directly but the idea is to simulate web traffic and use kafka as a total commit log for a web
  * interface
  *
  */
object StreamOut {

  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder
      .appName("StreamOut")
      .master("local")
      .getOrCreate()

    val data = spark.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/census.csv")

    data.printSchema()

    data.foreach((row) => {
      val rowMap = row.getValuesMap(row.schema.fieldNames)
      val rowJson = JSONObject(rowMap).toString()

      val req = POST(uri("http://localhost:3000/topics"), UrlForm(
        Map("topic" -> Seq("595a62a1641d21dedbfcb365"),
          "key"-> Seq("census"),
          "message"-> Seq(rowJson))
      ))
      val responseBody = client.expect[String](req)
      val run = responseBody.unsafeRun()

      Thread.sleep((Math.random() * 1000.0).toInt)
    })
  }

}

