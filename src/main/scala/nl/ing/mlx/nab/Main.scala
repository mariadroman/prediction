package nl.ing.mlx.nab

import kafka.serializer.StringDecoder
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {

  def main(args: Array[String]) {

    val params = Array("localhost:9092,localhost:9093,localhost:9094", "nab")

    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKMeansExample").set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
    val sscPrediction = new StreamingContext(sc, Seconds(5))
    val sscTraining = new StreamingContext(sc, Seconds(60))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> params(0))

    val linesTraining = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sscTraining, kafkaParams, params(1).split(",").toSet)
      .map(_._2).map(_.split(",")).map(x => Vectors.dense(timeToDouble(x(0)),x(1).toDouble))
    val linesPrediction = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sscPrediction, kafkaParams, params(1).split(",").toSet)
      .map(_._2).map(_.split(",")).map(x => Vectors.dense(timeToDouble(x(0)),x(1).toDouble))

    sscTraining.checkpoint("/tmp/modeler")
    sscPrediction.checkpoint("/tmp/predictor")

    val numDimensions = 2
    val numClusters = 5

    val model: StreamingKMeans = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    model.trainOn(linesTraining)
    model.predictOn(linesPrediction).print()

    sscPrediction.start()
    sscPrediction.awaitTermination()
    sscTraining.start()
    sscTraining.awaitTermination()
  }

  def timeToDouble(s: String): Double = {
    val time = s.trim.split(" ")(1).split(":")
    time(0).toDouble * 3600 + time(1).toDouble * 60 + time(2).toDouble
  }
}
