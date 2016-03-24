package sparkz.evaluation

import org.apache.spark.rdd.RDD
import sparkz.classifiers.{BinaryClassifierTrainer, FeaturesWithBooleanLabel}
import sparkz.utils.AppLogger

import scala.reflect.ClassTag
import scalaz.Scalaz._

object BinaryClassifierEvaluation {
  def crossValidation[Features, MetaData, UniqueKey: ClassTag](data: RDD[FeaturesWithBooleanLabel[Features, MetaData]],
                                                               k: Int,
                                                               classifier: BinaryClassifierTrainer[Features, MetaData],
                                                               seed: Long = 12345l,
                                                               uniqueId: MetaData => UniqueKey,
                                                               singleInference: Boolean = false)
                                                              (implicit logger: AppLogger = AppLogger.infoLevel()): RDD[(FeaturesWithBooleanLabel[Features, MetaData], Double)] =
    (for {
      i <- 1 to k
      trainingData = data.filter(_.metaData |> uniqueId |> (_.hashCode() % k == i))
      foldData = data.filter(_.metaData |> uniqueId |> (_.hashCode() % k != i))
      testData =
      if (singleInference)
        foldData.keyBy(_.metaData |> uniqueId).reduceByKey((record1, record2) => Set(record1, record2).head).values
      else foldData

      model = classifier.train(trainingData)
    } yield testData -> model).map {
      case (testData, model) => testData.map(testRecord => testRecord -> model.score(testRecord.features))
    }
    .reduce(_ ++ _)
}
