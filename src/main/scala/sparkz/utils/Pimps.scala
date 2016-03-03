package sparkz.utils

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.reflect.ClassTag
import scalaz.Scalaz._

case object Pimps {

  implicit class PimpedString(str: String) {
    def nonEmptyOption: Option[String] = str.nonEmpty.option(str)

    def nonEmptyOption[T](f: String => T): Option[T] = str.nonEmpty.option(str).map(f)
  }

  implicit def longToDateTime(ts: Long): DateTime = new DateTime(ts)

  implicit class PimpedIterable[A](i: Iterable[A]) {
    def maxesBy[B](f: A => B)(implicit cmp: Ordering[B]): Set[A] =
      if (i.nonEmpty) i.maxBy(f) |>
        ((max: A) => i.filter(x => f(x) == f(max)).toSet)
      else Set.empty
  }

  implicit class PimpedTupledSet[K, V](set: Set[(K, V)]) {
    def toMultiMap: Map[K, Set[V]] = set.groupBy(_._1).mapValues(_.map(_._2))
  }

  implicit class PimpedOptionMap[K, V](m: Map[Option[K], V]) {
    def flatMapOptionKeys = new PimpedMap(m).flatMapKeys(Option.option2Iterable)
  }

  implicit class PimpedMap[K, V](m: Map[K, V]) {
    def flatMapKeys[K2](f: (K => Iterable[K2])): Map[K2, V] = m.flatMap {
      case (k1, v) => f(k1).map(_ -> v)
    }

    def join[V2](that: Map[K, V2]): Map[K, (V, V2)] =
      m.flatMap(kv => that.get(kv._1).map(thatV => (kv._1, (kv._2, thatV))))

    def maxByValue(implicit cmp: Ordering[V]): (K, V) = m.maxByValue(v => v)

    def maxByValue[B](f: (V => B))(implicit cmp: Ordering[B]): (K, V) = m.maxBy(pair => f(pair._2))

    def maxesByValue(implicit cmp: Ordering[V]): List[(K, V)] = maxesByValue(v => v)

    def maxesByValue[B](f: (V => B))(implicit cmp: Ordering[B]): List[(K, V)] =
      m.maxBy(pair => f(pair._2)) |> ((max: (K, V)) => m.filter(_._2 == max._2).toList)

    def minByValue(implicit cmp: Ordering[V]): (K, V) = m.minByValue(v => v)

    def minByValue[B](f: (V => B))(implicit cmp: Ordering[B]): (K, V) = m.minBy(pair => f(pair._2))

    def minsByValue(implicit cmp: Ordering[V]): List[(K, V)] = minsByValue(v => v)

    def minsByValue[B](f: (V => B))(implicit cmp: Ordering[B]): List[(K, V)] =
      m.minBy(pair => f(pair._2)) |> ((max: (K, V)) => m.filter(_._2 == max._2).toList)
  }

  implicit class PimpedPairedList[K, V](l: List[(K, V)]) {
    def reduceByKey(func: (V, V) => V): Map[K, V] = l.groupBy(_._1).mapValues(_.map(_._2).reduce(func))
  }

  // TODO DRY with insights-engine code by making another shared project of some kind (could open source it!?)
  implicit class PimpedTupleIterable[T1, T2](l: Iterable[(T1, T2)]) {
    def mapTupled[U](f: (T1, T2) => U): Iterable[U] = l.map(f.tupled)

    def flatMapTupled[U](f: (T1, T2) => TraversableOnce[U]): Iterable[U] = l.flatMap(f.tupled)
  }

  implicit class PimpedMapDouble[K](m: Map[K, Double]) {
    def normalize: Map[K, Double] = m.values.sum |> (total => m.mapValues(_ / total))

    def productWithMap(m2: Map[K, Double]): Double = {
      (for {
        (k1, v1) <- m
        if m2.contains(k1)
      } yield v1 * m2(k1)).sum
    }
  }

  implicit class PimpedMapInt[K](m: Map[K, Int]) {
    def normalize: Map[K, Double] = m.values.sum |> (total => m.mapValues(_.toDouble / total))
  }

  implicit class PimpedSet[T](s: Set[T]) {
    def X[U](other: Set[U]): Set[(T, U)] = |@|(other).tupled.toSet

    def |@|[U](other: Set[U]) = s.toList |@| other.toList
  }

  implicit class PimpedRDD[T: ClassTag](rdd: RDD[T]) {
    def applyIf(condition: Boolean)(f: RDD[T] => RDD[T]): RDD[T] = if (condition) f(rdd) else rdd

    def thenDo[U](f: RDD[T] => U): RDD[T] = f(rdd) |> (_ => rdd)
  }
}
