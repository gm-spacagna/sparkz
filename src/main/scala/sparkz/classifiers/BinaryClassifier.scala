package sparkz.classifiers

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

trait BinaryClassifierTrainedModel[Features] extends Serializable {
  def score(featuresWindow: Features): Double
}

trait BinaryClassifierTrainer[Features] {
  def train(trainingData: RDD[_ <:FeaturesWithBooleanLabel[Features]]): BinaryClassifierTrainedModel[Features]
}

trait BinaryClassifierTrainedVectorModel extends Serializable {
  def score(vector: Vector): Double
}

trait BinaryClassifierVectorTrainer {
  def train(trainingData: RDD[LabeledPoint]): BinaryClassifierTrainedVectorModel
}
