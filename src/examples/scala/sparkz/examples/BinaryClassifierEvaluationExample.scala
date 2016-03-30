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
  def auc(data: RDD[(FeaturesWithBooleanLabel[UserFeatures] with MetaData[UserInfo])]): Map[BinaryClassifierTrainer[UserFeatures], Double] = {

    val vecDecisionTreeClassifier: BinaryClassifierVectorTrainer =
      DecisionTreeClassifierTrainer(impurity = "gini", maxDepth = 5, maxBins = 32)

    val eventsSubTransformer = TermFrequencyTransformer[String, UserFeatures](_.events)
    val categoriesSubTransformer = MultiOneHotTransformer[String, UserFeatures](_.attributes)
    val numericalsSubTransformer = OriginalNumericalsTransformer[String, UserFeatures](_.numericals)

    val classifiers: List[BinaryClassifierTrainer[UserFeatures]] = List(
      RandomBinaryClassifierTrainer(),
      BinaryClassifierTrainerWithTransformer(
        vec2classifier = vecDecisionTreeClassifier,
        transformer = eventsSubTransformer
      ),
      BinaryClassifierTrainerWithTransformer(
        vec2classifier = vecDecisionTreeClassifier,
        transformer = categoriesSubTransformer
      ),
      BinaryClassifierTrainerWithTransformer(
        vec2classifier = vecDecisionTreeClassifier,
        transformer = numericalsSubTransformer
      ),
      BinaryClassifierTrainerWithTransformer(
        vec2classifier = vecDecisionTreeClassifier,
        transformer = EnsembleTransformer(eventsSubTransformer, categoriesSubTransformer, numericalsSubTransformer)
      )
    )

    BinaryClassifierEvaluation.crossValidationScores(
      data = data,
      k = 10,
      classifiers = classifiers,
      uniqueId = (_: UserInfo).customerId,
      orderingField = (_: UserInfo).date.toDateTimeAtStartOfDay.getMillis,
      singleInference = true
    ).mapValues(
      _.map {
        case (features, score) => score -> features.isTrue
      }
      .confusions().areaUnderROC
    )
  }
}
