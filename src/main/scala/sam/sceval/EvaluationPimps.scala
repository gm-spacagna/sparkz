package sam.sceval

import BinUtils._
import org.apache.spark.Logging
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

import scala.collection.mutable
import scala.reflect.ClassTag

// TODO (long term) Abstract out the RDD part so we can have a List version and a ParIterable version.
// I.e. introduce a DistributedDataset type-class (or DD)

// TODO Another version where the number of models is huge, but there is a long (0.0, Boolean) tail which can be
// preaggregated to then allow for a reduceBy(model).
// Will be useful for evaluating matching algorithms or tiny-cluster clustering problems


object EvaluationPimps extends Logging {
  implicit class PimpedScoresAndLabelsRDD(scoreAndLabels: RDD[(Double, Boolean)]) {

    import PimpedScoresAndLabelsRDD._

    def confusions(cacheIntermediate: Option[StorageLevel] = Some(MEMORY_ONLY),
                   bins: Option[Int] = Some(1000),
                   recordsPerBin: Option[Long] = None): Seq[BinaryConfusionMatrix] =
      if (scoreAndLabels.isEmpty())
        Nil
      else
        scoreAndLabels.map(0 -> _).map(Map[Int, (Double, Boolean)](_))
        .confusionsByModel(cacheIntermediate, bins, recordsPerBin).map(_._2).collect().head

    /** Will incur 3 spark stages for bins != 0, 2 otherwise.
      * This method is `approx` in the sense the bin sizes and bin count can vary quite wildly. Use at own risk. */
    @deprecated("bin sizes & bin counts vary wildly. Use `confusions`.")
    def scoresAndConfusions(desiredBins: Int = 0): RDD[(Double, BinaryConfusionMatrix)] = {
      val binnedCounts: ScoresAndCounts =
        downSampleIfRequired(scoreAndLabels.combineByKey(
          createCombiner = new MutableBinaryLabelCount(0L, 0L) += (_: Boolean),
          mergeValue = (_: MutableBinaryLabelCount) += (_: Boolean),
          mergeCombiners = (_: MutableBinaryLabelCount) += (_: MutableBinaryLabelCount)
        ).sortByKey(ascending = false), desiredBins)

      val partitionwiseCumCounts: Array[MutableBinaryLabelCount] = partitionwiseCumulativeCounts(binnedCounts)
      val totalCount = partitionwiseCumCounts.last

      logInfo(s"Total counts: $totalCount")

      binnedCounts.mapPartitionsWithIndex(
        (index, partition) => partition.map {
          case (score, c) => (score, (partitionwiseCumCounts(index) += c).count)
        },
        preservesPartitioning = true)
      .map {
        case (score, cumCount) => (score, BinaryConfusionMatrix(cumCount, totalCount.count))
      }
    }
  }

  /** Model should extend AnyVal or Equals so that it makes sense to use this as a key.
    * Should scale reasonably well in the number of models. We return RDD because BCMs are computed in parallel for
    * each model, which ought to be a bit faster than a local computation for a large number of models.
    * This will ensure all bins except potentially the last bin are of equal size, consequently certain `bins` arguments
    * may be impossible to respect, in such cases the bin number that is closest to the desired bins is chosen.
    *
    * The first element of each Array[BinaryLabelCount] from `binaryLabelCounts` will be the total.
    *
    * This algorithm costs 4 stages and causes a job that runs the first 2.
    *
    * @param scoreAndLabelsByModel an RDD of Maps from the `Model` to a score-label pair.  It's assumed that for each
    *                              `Model` the total number of score-label pairs in the RDD is equal, if not then
    *                              the behaviour is unspecified and a warning is printed. */
  implicit class PimpedModelOutputsRDD[Model: ClassTag](scoreAndLabelsByModel: RDD[Map[Model, (Double, Boolean)]]) {

    import PimpedModelOutputsRDD._

    def confusionsByModel(cacheIntermediate: Option[StorageLevel] = Some(MEMORY_ONLY),
                          bins: Option[Int] = Some(1000),
                          recordsPerBin: Option[Long] = None): RDD[(Model, Array[BinaryConfusionMatrix])] =
      binaryLabelCounts(cacheIntermediate, bins, recordsPerBin)
      .mapValues(blcs => blcs.map(BinaryConfusionMatrix(_, blcs.head)))

    def binaryLabelCounts(cacheIntermediate: Option[StorageLevel] = Some(MEMORY_ONLY),
                          bins: Option[Int] = Some(1000),
                          recordsPerBin: Option[Long] = None): RDD[(Model, Array[BinaryLabelCount])] = {
      checkArgs(bins, recordsPerBin)

      scoreAndLabelsByModel.take(1).headOption.flatMap { _ =>
        val indexed: RDD[(Double, (Model, Boolean, Long))] = indexInPartition(scoreAndLabelsByModel)

        cacheIntermediate.foreach(indexed.persist)

        val lastIndexes: Array[Map[Model, Long]] = partitionLastIndexes(indexed)

        reindexByBin(indexed, lastIndexes, recordsPerBin, bins)
        .map(computeBLCs)
      }
      .getOrElse(scoreAndLabelsByModel.context.makeRDD[(Model, Array[BinaryLabelCount])](Nil))
    }
  }

  def checkArgs(bins: Option[Int] = Some(1000), recordsPerBin: Option[Long] = None): Unit = {
    require(bins.isDefined ^ recordsPerBin.isDefined, "Only one of bins or recordsPerBin can be specified")
    bins.foreach { b =>
      require(b > 0, "Doesn't make sense to request zero or less bins: " + b)
      require(b != 1, "Requesting 1 bin doesn't make sense. If you want the total use 2 bins and access " +
        "totalCount in BinaryLabelCounts")
    }
    recordsPerBin.foreach(r => require(r >= 0, "Doesn't make sense to request negative records per bin: " + r))
  }

  /** Companion object for `PimpedModelOutputsRDD` and methods only likely to be useful in this class.  Methods not
    * declared private since this is a functional context. */
  // We use mutability in these methods at the partition level to minimize pressure on GC
  object PimpedModelOutputsRDD {
    type Indexed[Model] = RDD[(Double, (Model, Boolean, Long))]

    def reindexByBin[Model: ClassTag](indexed: Indexed[Model],
                                      lastIndexes: Array[Map[Model, Long]],
                                      recordsPerBin: Option[Long],
                                      bins: Option[Int]): Option[RDD[(Model, Boolean, Int)]] = {
      val totalRecords =
        lastIndexes.flatMap(_.keys).toSet.map((model: Model) =>
          lastIndexes.filter(_.nonEmpty).flatMap(_.get(model).map(_ + 1)).sum)
        .toList match {
          case totalRecords :: Nil =>
            totalRecords
          case totalRecords :: _ :: _ =>
            logWarning("Total number of records for each model is not all equal.")
            totalRecords
        }

      logInfo("Total records: " + totalRecords)

      lastIndexes.find(_.nonEmpty).map { aNonEmptyPartition =>
        recordsPerBin.foreach(r => require(r < totalRecords, s"Cannot request $r records per bin as not enough " +
          s"records in total to make 2 bins: $totalRecords"))

        val numRecordsPerBin: Long = recordsPerBin.getOrElse(optimizeRecordsPerBin(totalRecords, bins.get))

        logInfo("Bins that will used: " + resultingBinNumber(numRecordsPerBin.toInt, totalRecords) +
          ", each with " + numRecordsPerBin + " records")

        reindexWithBinner(indexed, binnerFac(lastIndexes, numRecordsPerBin))
      }
    }

    def reindexWithBinner[Model: ClassTag](indexed: Indexed[Model],
                                           binner: (Model, Long, Int) => Int): RDD[(Model, Boolean, Int)] =
      indexed.mapPartitionsWithIndex((partitionIndex, partition) => partition.map {
        case (_, (model, label, index)) => (model, label, binner(model, index, partitionIndex))
      })

    /** The last element of each Array[BinaryLabelCount] will be the total */
    def computeBLCs[Model: ClassTag](indexedByBin: RDD[(Model, Boolean, Int)]): RDD[(Model, Array[BinaryLabelCount])] =
      indexedByBin.mapPartitions { partition =>
        val bins: mutable.Map[(Model, Int), MutableBinaryLabelCount] = mutable.Map()
        partition.foreach {
          case (model, label, bin) =>
            bins += ((model, bin) -> (bins.getOrElse((model, bin), MutableBinaryLabelCount()) += label))
        }
        bins.mapValues(_.count).toList.iterator
      }
      .reduceByKey(_ + _).map {
        case ((model, bin), count) => (model, (bin, count))
      }
      .groupByKey()
      .mapValues(_.toArray.sortBy(-_._1).map(_._2).scan(BinaryLabelCount())(_ + _).drop(1).reverse)

    def indexInPartition[Model: ClassTag](scoreAndLabelsByModel: RDD[Map[Model, (Double, Boolean)]]): Indexed[Model] =
      scoreAndLabelsByModel.flatMap(identity).map {
        case (model, (score, label)) => (score, (model, label))
      }
      .sortByKey()
      .mapPartitions(partition => {
        val modelToCount: mutable.Map[Model, Long] = mutable.Map()
        partition.map {
          case (score, (model, label)) =>
            val index = modelToCount.getOrElse(model, 0L)
            modelToCount += (model -> (index + 1))
            // TODO Determine if keeping the score here is actually necessary - I don't think it makes sense
            (score, (model, label, index))
        }
      }, preservesPartitioning = true)

    // Uses memory O(models x partitions)
    def partitionLastIndexes[Model: ClassTag](indexed: Indexed[Model]): Array[Map[Model, Long]] =
      indexed.mapPartitions { partition =>
        val modelToCount: mutable.Map[Model, Long] = mutable.Map()
        partition.foreach {
          case (_, (model, _, index)) => modelToCount += model -> index
        }
        Iterator(modelToCount.toMap)
      }
      .collect()
  }

  implicit class PimpedConfusionsSeq(confusions: Seq[BinaryConfusionMatrix]) {
    def roc: Seq[(Double, Double)] =
      (0.0, 0.0) +: confusions.map(bcm => (bcm.falsePositiveRate, bcm.recall)) :+(1.0, 1.0)

    def precisionByVolume: Seq[(Double, Double)] = confusions.map(bcm => (bcm.volume, bcm.precision))
    def recallByVolume: Seq[(Double, Double)] = confusions.map(bcm => (bcm.volume, bcm.recall))
    def precisionRecallCurve: Seq[(Double, Double)] = (0.0, 1.0) +: confusions.map(bcm => (bcm.recall, bcm.precision))
    def areaUnderROC: Double = AreaUnderCurve(roc)
    def areaUnderPR: Double = AreaUnderCurve(precisionRecallCurve)
  }

  /** These are left as RDDs as per the original API, but they will be small data that's best returned to the driver
    * for subsequent processing */
  implicit class PimpedScoresAndConfusionsRDD(confusions: RDD[(Double, BinaryConfusionMatrix)]) {
    def roc(): RDD[(Double, Double)] = {
      val rocCurve = confusions.map(_._2).map(bcm => (bcm.falsePositiveRate, bcm.recall))
      val sc = confusions.context
      val first = sc.makeRDD(Seq((0.0, 0.0)), 1)
      val last = sc.makeRDD(Seq((1.0, 1.0)), 1)
      new UnionRDD[(Double, Double)](sc, Seq(first, rocCurve, last))
    }

    def precisionRecallCurve(): RDD[(Double, Double)] =
      confusions.context.makeRDD(Seq((0.0, 1.0)), 1)
      .union(confusions.map(_._2).map(bcm => (bcm.recall, bcm.precision)))

    def thresholds(): RDD[Double] = confusions.map(_._1)
    def areaUnderROC(): Double = AreaUnderCurve(roc())
    def areaUnderPR(): Double = AreaUnderCurve(precisionRecallCurve())
    def precisionByThreshold(): RDD[(Double, Double)] = confusions.mapValues(_.precision)
    def recallByThreshold(): RDD[(Double, Double)] = confusions.mapValues(_.recall)
    @deprecated("Don't use meaningless measures, use something that has a direct probabilistic meaning. See README.md")
    def f1MeasureByThreshold(beta: Double = 1.0): RDD[(Double, Double)] = confusions.mapValues(_.f1Measure(beta))
  }

  object PimpedScoresAndLabelsRDD {
    type ScoresAndCounts = RDD[(Double, MutableBinaryLabelCount)]

    // This doesn't scale in the number of bins since it all happens on the driver node
    def partitionwiseCumulativeCounts(binnedCounts: ScoresAndCounts): Array[MutableBinaryLabelCount] =
      binnedCounts.values.mapPartitions { partition =>
        val agg = MutableBinaryLabelCount()
        partition.foreach(agg +=)
        Iterator(agg)
      }
      .collect()
      .scanLeft(MutableBinaryLabelCount())(_.clone += _)

    def downSample(grouping: Int, sortedCounts: ScoresAndCounts): ScoresAndCounts =
      sortedCounts.mapPartitions(_.grouped(grouping.toInt).map {
        case group@((firstScore, _) :: _) =>
          val agg = new MutableBinaryLabelCount()
          group.map(_._2).foreach(agg +=)
          (firstScore, agg)
      })

    def downSampleIfRequired(sortedCounts: ScoresAndCounts, desiredBins: Int): ScoresAndCounts =
      if (desiredBins == 0) sortedCounts
      else {
        val countsSize = sortedCounts.count()
        countsSize / desiredBins match {
          case g if g < 2 =>
            logInfo(s"Curve is too small ($countsSize) for $desiredBins bins to be useful")
            sortedCounts
          case g if g >= Int.MaxValue =>
            logWarning(s"Curve too large ($countsSize) for $desiredBins bins; capping at ${Int.MaxValue}")
            downSample(Int.MaxValue, sortedCounts)
          case g =>
            downSample(g.toInt, sortedCounts)
        }
      }
  }
}
