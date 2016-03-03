package sparkz.utils

import scalaz.Monoid

case object StatCounterMonoid extends Monoid[StatCounter] {
  def zero: StatCounter = ZeroStatCounter

  def append(f1: StatCounter, f2: => StatCounter): StatCounter = f1 merge f2
}

object ZeroStatCounter extends StatCounter(0, 0.0, 0.0, Double.PositiveInfinity, Double.NegativeInfinity)

case object StatCounter {
  def apply(values: TraversableOnce[Double]): StatCounter = values.foldLeft(ZeroStatCounter: StatCounter)(_ merge _)

  def apply(value: Double): StatCounter = apply(List(value))

  implicit val monoid: Monoid[StatCounter] = StatCounterMonoid
}

case class StatCounter(n: Long, sum: Double, sos: Double, min: Double, max: Double) {
  def merge(other: StatCounter) = StatCounter(n + other.n, sum + other.sum, sos + other.sos,
    math.min(min, other.min), math.max(max, other.max))

  def merge(value: Double) = StatCounter(n + 1, sum + value, sos + (value * value),
    math.min(min, value), math.max(max, value))

  def merge(values: TraversableOnce[Double]): StatCounter = values.foldLeft(this)(_ merge _)

  def count = n

  def mean = sum / n

  def variance = if (n > 1) (sos - n * mean * mean) / (n - 1) else Double.NaN

  def stdev = math.sqrt(variance)

  def stderr = stdev / math.sqrt(n)

  override def toString = "(count: %d, mean: %f, stdev: %f, min: %f, max: %f)".format(count, mean, stdev, max, min)
}
