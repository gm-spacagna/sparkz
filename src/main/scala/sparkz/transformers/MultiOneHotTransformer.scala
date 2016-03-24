package sparkz.transformers

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case object MultiOneHotTransformer {
  def apply[Key: ClassTag: Ordering, Features](attributes: Features => Map[Key, String]) =
    new SubFeaturesTransformer[Map[Key, String], Features] {
      def subFeatures(features: Features): Map[Key, String] = attributes(features)
      def subFeaturesToVector(trainingData: RDD[Map[Key, String]]): (Map[Key, String]) => Vector = {
        val attributeToIndexBV: Broadcast[Map[(Key, String), Int]] = trainingData.context.broadcast(
          trainingData.flatMap(identity).distinct().collect().sorted.zipWithIndex.toMap
        )

        (attributes: Map[Key, String]) => Vectors.sparse(
          size = attributeToIndexBV.value.size,
          elements = attributes.toSeq.flatMap(attribute =>
            attributeToIndexBV.value.get(attribute).map(_ -> 1.0)
          ))
      }
    }
}
