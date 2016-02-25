package nl.ing.mlx.nab

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

object DataNormalizer {

  def minTime = 0.0
  def maxTime = 24.0 * 60.0 * 60.0

  def minValue = -40.0
  def maxValue = 90.0

  def normalizeTime(t: Double) = (t - minTime) / (maxTime - minTime)
  def normalizeValue(v: Double) = (v - minValue) / (maxValue - minValue)

  def transform(data: Vector): Vector = {
    Vectors.dense(normalizeTime(data.toArray(0)), normalizeValue(data.toArray(1)))
  }

}
