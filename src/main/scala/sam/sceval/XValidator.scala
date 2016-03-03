package sam.sceval

import org.apache.spark.rdd.RDD
import EvaluationPimps._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import scala.util.Random

/** x-validator that uses near exact same size folds */
case class XValidator(folds: Int = 10, 
                      evalBins: Option[Int] = Some(1000),
                      evalCacheIntermediate: Option[StorageLevel] = Some(MEMORY_ONLY),
                      evalRecordsPerBin: Option[Long] = None) {

  def trainWithExample(fold: Int, modelIndex: Int): Boolean = fold != modelIndex
  def scoreWithExample(fold: Int, modelIndex: Int): Boolean = fold == modelIndex

  /** Randomly enumerates all values such that each fold will have the same number of elements + / - 1.
    * It is then up to the user to decide how to use this to train their models (user can use the helper methods
    * `trainWithExample` and `scoreWithExample` to ensure a consistent approach)
    *
    * Strictly speaking there are edge cases where this will not generate random splits.  Particularly when partitions
    * consist of a very small number of examples. */
  def split[Features](featuresAndLabels: RDD[(Features, Boolean)]): RDD[(Int, Features, Boolean)] = {
    val upToFolds = featuresAndLabels.take(folds).length
    require(upToFolds == folds, s"Not enough records ($upToFolds) for $folds folds")
    featuresAndLabels.mapPartitions(new Random().shuffle(_)).zipWithIndex().map {
      case ((f, l), i) => ((i % folds).toInt, f, l)
    }
  }

  def evaluate(scoresAndLabelsByModel: RDD[(Int, Double, Boolean)]): Array[BinaryConfusionMatrix] =
    scoresAndLabelsByModel.map(p => Map(p._1 -> (p._2, p._3)))
    .confusionsByModel(evalCacheIntermediate, evalBins, evalRecordsPerBin).map(_._2)
    .flatMap(_.zipWithIndex.map(_.swap)).reduceByKey(_ + _).collect().sortBy(_._1).map(_._2)

  def xval[Features](trainAndScoreByModel: RDD[(Int, Features, Boolean)] => RDD[(Int, Double, Boolean)],
                     featuresAndLabel: RDD[(Features, Boolean)]): Array[BinaryConfusionMatrix] =
    evaluate(trainAndScoreByModel(split(featuresAndLabel)))
}
