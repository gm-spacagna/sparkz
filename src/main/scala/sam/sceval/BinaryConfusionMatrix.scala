package sam.sceval

case class MutableBinaryLabelCount(var numPositives: Long = 0L, var numNegatives: Long = 0L) {
  def +=(label: Boolean): MutableBinaryLabelCount = {
    if (label) numPositives += 1L else numNegatives += 1L
    this
  }

  def +=(other: MutableBinaryLabelCount): MutableBinaryLabelCount = {
    numPositives += other.numPositives
    numNegatives += other.numNegatives
    this
  }

  override def clone: MutableBinaryLabelCount = new MutableBinaryLabelCount(numPositives, numNegatives)

  def count: BinaryLabelCount = BinaryLabelCount(numPositives, numNegatives)
  def total: Long = numPositives + numNegatives
}

case class BinaryLabelCount(numPositives: Long = 0L, numNegatives: Long = 0L) {
  def total: Long = numPositives + numNegatives

  def +(label: Boolean): BinaryLabelCount =
    if (label) copy(numPositives = numPositives + 1L) else copy(numNegatives = numNegatives + 1L)

  def +(other: BinaryLabelCount): BinaryLabelCount =
    BinaryLabelCount(numPositives + other.numPositives, numNegatives + other.numNegatives)
}

case class BinaryConfusionMatrix(tp: Long, fp: Long, tn: Long, fn: Long) {
  def total: Long = tp + fp + tn + fn

  def actualPositives: Long = tp + fn
  def actualNegatives: Long = fp + tn

  def predictedPositives: Long = tp + fp
  def predictedNegatives: Long = tn + fn

  def volume: Double = predictedPositives.toDouble / total

  /** The 'probability' the label is actually True given we predict True */
  def precision: Double = if (predictedPositives == 0) 1.0 else tp.toDouble / predictedPositives

  /** The 'probability' we will predict True given the label is actually True */
  def recall: Double = if (actualPositives == 0) 0.0 else tp.toDouble / actualPositives

  /** Actual probability of True */
  def prior: Double = actualPositives.toDouble / total

  /** The 'probability' we will predict True */
  def predictedPrior: Double = (tp + fp).toDouble / total

  /** How many times the predictor is better than random. One can usually map this number directly to
    * savings / profit making it have business meaning, unlike most measures. */
  def uplift: Double = (tp * total).toDouble / ((tp + fp) * (tp + fn))

  def specificity: Double = tn.toDouble / actualNegatives
  def negativePredictiveValue: Double = tn.toDouble / predictedNegatives
  def fallOut: Double = fp.toDouble / actualNegatives
  def falseDiscoveryRate: Double = fp.toDouble / predictedPositives
  def falsePositiveRate: Double = if (actualNegatives == 0) 0.0 else fp.toDouble / actualNegatives
  def accuracy: Double = (tp + tn).toDouble / total

  def +(other: BinaryConfusionMatrix): BinaryConfusionMatrix =
    BinaryConfusionMatrix(other.tp + tp, other.fp + fp, other.tn + tn, other.fn + fn)

  @deprecated("Don't use meaningless measures, use something that has a direct probabilistic meaning. See README.md")
  def f1Measure(beta: Double = 1.0): Double = {
    val beta2 = beta * beta
    if (precision + recall == 0) 0.0 else (1.0 + beta2) * (precision * recall) / (beta2 * precision + recall)
  }
}

object BinaryConfusionMatrix {
  def apply(count: BinaryLabelCount, totalCount: BinaryLabelCount): BinaryConfusionMatrix = BinaryConfusionMatrix(
    tp = count.numPositives,
    fp = count.numNegatives,
    tn = totalCount.numNegatives - count.numNegatives,
    fn = totalCount.numPositives - count.numPositives
  )
}


