package sparkz.evaluation

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._

import scalaz.Scalaz._

object MAP {
  def apply[T](n: Int = 100, recommendations: RDD[(Long, List[T])], evaluation: RDD[(Long, Set[T])]): Double =
    recommendations.join(evaluation).values.map {
      case (recommendedLikes, trueLikes) => recommendedLikes.take(n).zipWithIndex.foldLeft(0, 0.0) {
        case ((accLikes, accPrecision), (postId, k)) if trueLikes(postId) =>
          (accLikes + 1, accPrecision + ((accLikes + 1).toDouble / (k + 1)))
        case ((accLikes, accPrecision), _) => (accLikes, accPrecision)
      }._2 / math.min(trueLikes.size, n)
    } |> (apn => {
      val count = apn.count()
      if (count > 0) apn.reduce(_ + _) / count else 0
    })
}
