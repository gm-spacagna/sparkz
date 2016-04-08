package sparkz.transformers

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case object OriginalNumericalsTransformer {
  def apply[Key: ClassTag: Ordering, Features](numericals: Features => Map[Key, Double]) =
    new SubFeaturesTransformer[Map[Key, Double], Features] {
      def subFeatures(features: Features): Map[Key, Double] = numericals(features)
      def subFeaturesToVector(trainingData: RDD[Map[Key, Double]]): (Map[Key, Double]) => Vector = {
        val keyToIndexBV: Broadcast[Map[Key, Int]] = trainingData.context.broadcast(
          trainingData.flatMap(_.keySet).distinct().collect().sorted.zipWithIndex.toMap
        )

        (numericals: Map[Key, Double]) => Vectors.sparse(
          size = keyToIndexBV.value.size,
          elements = numericals.toSeq.flatMap {
            case (key, value) => keyToIndexBV.value.get(key).map(_ -> value)
          })
      }
    }
}

