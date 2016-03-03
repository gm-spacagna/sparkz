package sam.sceval

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD

/** Computes the area under the curve (AUC) using the trapezoidal rule. */
@deprecated("Don't use meaningless measures, use something that has a direct probabilistic meaning. See README.md")
object AreaUnderCurve {
  def trapezoid(points: Seq[(Double, Double)]): Double = {
    val (x1, x2) :: (y1, y2) :: Nil = points
    (y1 - x1) * (y2 + x2) / 2.0
  }

  def apply(curve: RDD[(Double, Double)]): Double = curve.sliding(2).aggregate(0.0)(
    seqOp = (auc: Double, points: Array[(Double, Double)]) => auc + trapezoid(points.toList),
    combOp = _ + _
  )

  def apply(curve: Iterable[(Double, Double)]): Double =
    curve.toIterator.sliding(2).withPartial(false).aggregate(0.0)(
      seqop = (auc: Double, points: Seq[(Double, Double)]) => auc + trapezoid(points),
      combop = _ + _
    )
}
