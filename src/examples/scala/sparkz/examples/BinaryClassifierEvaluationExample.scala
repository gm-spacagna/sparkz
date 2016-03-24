package sparkz.examples

import org.apache.spark.rdd.RDD
import org.joda.time.LocalDate
import sam.sceval.EvaluationPimps._
import sparkz.classifiers._
import sparkz.evaluation.BinaryClassifierEvaluation
import sparkz.transformers._

case class UserInfo(customerId: Long, date: LocalDate)
case class UserFeatures(events: List[String], attributes: Map[String, String], numericals: Map[String, Double])
case class UserFeaturesWithBooleanLabel(features: UserFeatures,
                                        metaData: UserInfo,
                                        isTrue: Boolean) extends FeaturesWithBooleanLabel[UserFeatures] with MetaData[UserInfo]

object BinaryClassifierEvaluationExample {
  def auc(data: RDD[(FeaturesWithBooleanLabel[UserFeatures] with MetaData[UserInfo])]): Double = {

    val vectorClassifier: BinaryClassifierVectorTrainer =
      DecisionTreeClassifierTrainer(impurity = "gini", maxDepth = 5, maxBins = 32)

    val eventsSubTransformer = TermFrequencyTransformer[String, UserFeatures](_.events)
    val categoriesSubTransformer = MultiOneHotTransformer[String, UserFeatures](_.attributes)
    val numericalsSubTransformer = OriginalNumericalsTransformer[String, UserFeatures](_.numericals)

    val classifier: BinaryClassifierTrainer[UserFeatures] = BinaryClassifierTrainerWithTransformer(
      vec2classifier = vectorClassifier,
      transformer = EnsembleTransformer(eventsSubTransformer, categoriesSubTransformer, numericalsSubTransformer)
    )

    BinaryClassifierEvaluation.crossValidation(
      data = data,
      k = 10,
      classifier = classifier,
      uniqueId = (_: UserInfo).customerId,
      orderingField = (_: UserInfo).date.toDateTimeAtStartOfDay.getMillis,
      singleInference = true
    ).map {
      case (features, score) => score -> features.isTrue
    }
    .confusions().areaUnderROC
  }
}
