package nl.ing.mlx.nab

import java.io.{BufferedOutputStream, PrintWriter}

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.clustering.{StreamingKMeansModel, StreamingKMeans}
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql.EsSparkSQL
import spire.math.Interval

import scala.util.{Failure, Success, Try}

object Main {

  val params = Array("localhost:9092,localhost:9093,localhost:9094", "nab")

  val period: Long = 15//24 * 60 * 60

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StreamingKMeansExample")
      .set("es.index.auto.create", "true")
      .set("es.nodes","localhost")
      .set("es.port","9200")
    //.set("spark.driver.allowMultipleContexts","true")

//    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(period))
//    val sqlContext = new SQLContext(sc)

//    import sqlContext.implicits._

    val kafkaParams = Map[String, String]("metadata.broker.list" -> params(0))

    val data = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, params(1).split(",").toSet)
      .map(_._2).map(_.split(",")).map(x => Vectors.dense(timeToDouble(x(0)),x(1).toDouble))

    //Normalize data
    val normalizedData: DStream[Vector] = data.map(DataNormalizer.transform)

    //print normalizedData
    normalizedData.foreachRDD(rdd => rdd.foreach(println))

    ssc.checkpoint("/tmp/predictor")

    val numDimensions = 2
    val numClusters = 5
    val decayFactor = 0.8

    val model: StreamingKMeans = new StreamingKMeans()
      .setK(numClusters)
      .setHalfLife(5760,"batches") //every 15 seconds 1 batch coming with information of 3 points => 86400/15 = 5760
//      .setDecayFactor(decayFactor)
      .setRandomCenters(numDimensions, 0.0)

    val clusterCenters: Array[Vector] = model.latestModel().clusterCenters

    val prediction: DStream[Int] = model.predictOn(normalizedData)
    prediction.print()

    normalizedData.foreachRDD {
      rdd =>
        rdd.foreach {
          vector =>
            println("Current element => " + vector)
            val centroids = model.latestModel().clusterCenters
            val prediction = model.latestModel().predict(vector)
            centroids.foreach(println)
            println("Index for the centroid is => " + prediction)
            println("Centroid for current element => " + centroids(prediction))
            val distanceX = math.abs(centroids(prediction).toArray(0) - vector.toArray(0))
            val distanceY = math.abs(centroids(prediction).toArray(1) - vector.toArray(1))
            println("Distance from element to centroid => [" + distanceX + "," + distanceY + "]")
        }
    }

    model.trainOn(normalizedData)

    ssc.start()
    ssc.awaitTermination()

  }

  def timeToDouble(s: String): Double = {
    val time = Try(s.trim.split(" ")(1).split(":")) match {
      case Success(v) => v
      case Failure(_) => Array("-1")
    }
    Try(time(0).toDouble * 3600 + time(1).toDouble * 60 + time(2).toDouble) match {
      case Success(v) => v
      case Failure(_) => -1.0
    }
  }
}
