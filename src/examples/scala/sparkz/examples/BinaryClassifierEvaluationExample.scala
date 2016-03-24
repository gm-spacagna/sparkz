package sparkz.examples

import org.apache.spark.rdd.RDD
import sam.sceval.EvaluationPimps._
import sparkz.classifiers._
import sparkz.evaluation.BinaryClassifierEvaluation
import sparkz.transformers._

case class UserMetaData(customerId: Long)
case class UserFeatures(events: List[String], attributes: Map[String, String], numericals: Map[String, Double])
case class UserFeaturesWithBooleanLabel(features: UserFeatures,
                                        metaData: UserMetaData,
                                        isTrue: Boolean) extends FeaturesWithBooleanLabel[UserFeatures, UserMetaData]

object BinaryClassifierEvaluationExample {
  def auc(data: RDD[FeaturesWithBooleanLabel[UserFeatures, UserMetaData]]): Double = {

    val vectorClassifier: BinaryClassifierVectorTrainer =
      DecisionTreeClassifierTrainer(impurity = "gini", maxDepth = 5, maxBins = 32)

    val eventsTransformer = TermFrequencyTransformer[String, UserFeatures, UserMetaData](_.events)
    val categoriesTransformer = MultiOneHotTransformer[String, UserFeatures, UserMetaData](_.attributes)
    val numericalsTransformer = OriginalNumericalsTransformer[String, UserFeatures, UserMetaData](_.numericals)

    val classifier: BinaryClassifierTrainer[UserFeatures, UserMetaData] = BinaryClassifierTrainerWithTransformer(
      vec2classifier = vectorClassifier,
      transformer = EnsembleTransformer(eventsTransformer, categoriesTransformer, numericalsTransformer)
    )

    BinaryClassifierEvaluation.crossValidation(
      data = data, k = 10, classifier = classifier, uniqueId = (_: UserMetaData).customerId).map {
      case (features, score) => score -> features.isTrue
    }
    .confusions().areaUnderROC
  }
}
