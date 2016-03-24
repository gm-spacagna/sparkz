package sparkz.classifiers

import org.apache.spark.rdd.RDD

import scala.util.Random

case class RandomBinaryClassifierTrainer[Features, MetaData](seed: Option[Long] = None) extends BinaryClassifierTrainer[Features, MetaData] {
  def train(trainingData: RDD[FeaturesWithBooleanLabel[Features, MetaData]]): BinaryClassifierTrainedModel[Features] =
    new BinaryClassifierTrainedModel[Features] {
      def score(vector: Features): Double = seed.map(new Random(_)).getOrElse(Random).nextDouble
    }
}
