package sparkz.classifiers

import org.apache.spark.rdd.RDD

import scala.util.Random

case class RandomBinaryClassifierTrainer[Features, MetaData](seed: Option[Long] = None) extends BinaryClassifierTrainer[Features] {
  def train(trainingData: RDD[_ <: FeaturesWithBooleanLabel[Features]]): BinaryClassifierTrainedModel[Features] =
    new BinaryClassifierTrainedModel[Features] {
      def score(vector: Features): Double = seed.map(new Random(_)).getOrElse(Random).nextDouble
    }
}
