package sparkz.transformers

import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait FeaturesTransformer[Features] extends Serializable {
  def featuresToVector(trainingData: RDD[Features]): Features => Vector
}

abstract class SubFeaturesTransformer[SubFeatures: ClassTag, Features] extends FeaturesTransformer[Features] {
  def subFeatures(features: Features): SubFeatures

  def subFeaturesToVector(trainingData: RDD[SubFeatures]): SubFeatures => Vector

  def featuresToVector(trainingData: RDD[Features]): Features => Vector = {
    val toVector = subFeaturesToVector(trainingData.map(subFeatures))

    (features: Features) => toVector(subFeatures(features))
  }
}

case object EnsembleTransformer {
  def concatenateVectors(v1: Vector, v2: Vector): Vector = (v1, v2) match {
    case (v1: SparseVector, v2: SparseVector) => Vectors.sparse(
      size = v1.size + v2.size,
      indices = v1.indices ++ v2.indices.map(_ + v1.size),
      values = v1.values ++ v2.values
    )
    case (v1: SparseVector, v2: DenseVector) => Vectors.sparse(
      size = v1.size + v2.size,
      indices = v1.indices ++ (v1.size until (v1.size + v2.size)),
      values = v1.values ++ v2.values
    )
    case (v1: DenseVector, v2: SparseVector) => Vectors.sparse(
      size = v1.size + v2.size,
      indices = (0 until v1.size).toArray ++ v2.indices.map(_ + v1.size),
      values = v1.values ++ v2.values
    )
    case (v1: DenseVector, v2: DenseVector) =>
      Vectors.dense(values = v1.values ++ v2.values)
  }
}

case class EnsembleTransformer[Features](subTransformer1: SubFeaturesTransformer[_, Features],
                                         otherSubTransformers: SubFeaturesTransformer[_, Features]*) extends FeaturesTransformer[Features] {
  def featuresToVector(trainingData: RDD[Features]): Features => Vector = {
    (features: Features) =>
      (subTransformer1 +: otherSubTransformers).map(_.featuresToVector(trainingData))
      .map(_.apply(features)).reduce(EnsembleTransformer.concatenateVectors)
  }
}