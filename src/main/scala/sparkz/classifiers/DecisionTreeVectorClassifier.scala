package sparkz.classifiers

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.FeatureType
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.rdd.RDD

case class DecisionTreeClassifierTrainer(impurity: String,
                                         maxDepth: Int,
                                         maxBins: Int) extends BinaryClassifierVectorTrainer {
  def train(trainingData: RDD[LabeledPoint]): BinaryClassifierTrainedVectorModel = {

    val model = DecisionTree.trainClassifier(
      input = trainingData,
      numClasses = 2,
      categoricalFeaturesInfo = Map.empty[Int, Int],
      impurity = impurity,
      maxDepth = maxDepth,
      maxBins = maxBins
    )

    val topNode = model.topNode

    new BinaryClassifierTrainedVectorModel {
      def score(vector: Vector): Double = {
        DecisionTreeInference.predictProb(topNode)(vector)
      }
    }
  }
}

case object DecisionTreeInference {
  def predictProb(node: Node)(features: Vector): Double = {
    if (node.isLeaf) {
      if (node.predict.predict == 1.0) node.predict.prob else 1.0 - node.predict.prob
    } else {
      assert(node.split.get.featureType == FeatureType.Continuous)
      if (features(node.split.get.feature) <= node.split.get.threshold) {
        predictProb(node.leftNode.get)(features)
      } else {
        predictProb(node.rightNode.get)(features)
      }
    }
  }
}
