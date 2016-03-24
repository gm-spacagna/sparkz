package sparkz.classifiers

trait FeaturesWithBooleanLabel[Features, MetaData] {
  def features: Features
  def metaData: MetaData
  def isTrue: Boolean
}
