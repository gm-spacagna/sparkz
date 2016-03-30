package sparkz.classifiers

import org.apache.spark.rdd.RDD

import scala.util.Random

case class RandomBinaryClassifierTrainer[Features](seed: Long = 12345L) extends BinaryClassifierTrainer[Features] {
  def train(trainingData: RDD[_ <: FeaturesWithBooleanLabel[Features]]): BinaryClassifierTrainedModel[Features] = {
    val random = new Random(seed)
    new BinaryClassifierTrainedModel[Features] {
      def score(vector: Features): Double = random.nextDouble
    }
  }
}
