package sparkz.classifiers

import org.apache.spark.rdd.RDD

import scala.util.Random

case class RandomBinaryClassifierTrainer[Features](seed: Option[Long] = None) extends BinaryClassifierTrainer[Features] {
  def train(trainingData: RDD[_ <: FeaturesWithBooleanLabel[Features]]): BinaryClassifierTrainedModel[Features] = {
    val random: Option[Random] = seed.map(new Random(_))
    new BinaryClassifierTrainedModel[Features] {
      def score(vector: Features): Double = random.getOrElse(new Random()).nextDouble
    }
  }
}
