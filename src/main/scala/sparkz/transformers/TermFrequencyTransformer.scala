package sparkz.transformers

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case object TermFrequencyTransformer {
  def apply[Term: ClassTag: Ordering, Features](terms: Features => List[Term]) = new SubFeaturesTransformer[List[Term], Features] {
    def subFeatures(features: Features): List[Term] = terms(features)

    def subFeaturesToVector(trainingData: RDD[List[Term]]): (List[Term]) => Vector = {
      val termToIndexBV: Broadcast[Map[Term, Int]] = trainingData.context.broadcast(
        trainingData.flatMap(identity).distinct().collect().sorted.zipWithIndex.toMap
      )

      (terms: List[Term]) =>
        Vectors.sparse(termToIndexBV.value.size, terms.groupBy(identity).mapValues(_.size).flatMap {
          case (term, count) => termToIndexBV.value.get(term).map(_ -> count.toDouble)
        }.toSeq)
    }
  }
}
