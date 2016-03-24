package sparkz.classifiers

trait FeaturesWithBooleanLabel[Features] {
  def features: Features
  def isTrue: Boolean
}

trait MetaData[MetaData] {
  def metaData: MetaData
}

object EmptyMetaData extends MetaData[Unit] {
  def metaData: Unit = ()
}
