package sparkz.classifiers

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import sparkz.transformers.FeaturesTransformer

import scala.reflect.ClassTag

case object BinaryClassifierTrainerWithTransformer {
  def labeledPoint[Features: ClassTag](featuresWithBooleanLabel: FeaturesWithBooleanLabel[Features],
                                       toVector: Features => Vector): LabeledPoint = featuresWithBooleanLabel match {
    case featuresWithLabel =>
      LabeledPoint(if (featuresWithLabel.isTrue) 1.0 else 0.0, toVector(featuresWithLabel.features))
  }

  def apply[Features: ClassTag](vec2classifier: BinaryClassifierVectorTrainer,
                                transformer: FeaturesTransformer[Features]): BinaryClassifierTrainer[Features] =
    new BinaryClassifierTrainer[Features] {
      def train(trainingData: RDD[_ <: FeaturesWithBooleanLabel[Features]]): BinaryClassifierTrainedModel[Features] = {
        val toVector = transformer.featuresToVector(trainingData.map(_.features))

        val model = vec2classifier.train(trainingData.map(labeledPoint(_, toVector)))
        new BinaryClassifierTrainedModel[Features] {
          def score(featuresWindow: Features): Double = model.score(toVector(featuresWindow))
        }
      }
    }
}
