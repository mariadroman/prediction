package nl.ing.mlx.nab

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary

import scala.math._

/**
  * This is a case class wrapper for the MultivariateStatisticalSummary, storing values instead of running computations
  */
case class MVSS(mean: Vector, count: Long, numNonzeros: Vector,
                normL1: Vector, normL2: Vector, variance: Vector, stdev: Vector,
                max: Vector, min: Vector, mid: Vector, width: Vector
               ) extends MultivariateStatisticalSummary

object MVSS {

  def apply(source : MultivariateStatisticalSummary): MVSS = {

    val stdev = source.variance.toArray.map(sqrt)
    val mid = source.max.toArray.zip(source.min.toArray).map { case (xM, xm) => (xM + xm) / 2 }
    val width = source.max.toArray.zip(source.min.toArray).map { case (xM, xm) => abs(xM - xm) }

    new MVSS(source.mean, source.count, source.numNonzeros,
      source.normL1, source.normL2, source.variance, Vectors.dense(stdev),
      source.max, source.min, Vectors.dense(mid), Vectors.dense(width))
  }

}
