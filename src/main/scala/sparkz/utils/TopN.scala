package sparkz.utils

import scala.reflect.ClassTag

object TopElements {
  def topN[T: ClassTag](elems: Iterable[T])(scoreFunc: T => Double, n: Int): List[T] =
    elems.foldLeft((Set.empty[(T, Double)], Double.MaxValue)) {
      case (accumulator@(topElems, minScore), elem) =>
        val score = scoreFunc(elem)
        if (topElems.size < n)
          (topElems + (elem -> score), math.min(minScore, score))
        else if (score > minScore) {
          val newTopElems = topElems - topElems.minBy(_._2) + (elem -> score)
          (newTopElems, newTopElems.map(_._2).min)
        }
        else accumulator
    }
      ._1.toList.sortBy(_._2).reverse.map(_._1)
}
