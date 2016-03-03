package sparkz.utils

import VPTree._

import scala.reflect.ClassTag

case class VPTree[T1: ClassTag, T2: ClassTag](root: Tree[T1, T2], distance: Distance[T1]) {
  def nearest(t: T1, maxDist: Double) = root.nearN(t, maxDist, distance)

  def approximateNearest(t: T1): (T1, T2) = root.approxNear(t, distance)

  def approximateNearestN(t: T1, n: Int): Array[(T1, T2)] = root.approxNearN(t, n, distance)
}

// Adapted version of https://github.com/kaja47/sketches/blob/master/VPTree.scala
case object VPTree {
  type Distance[T1] = (T1, T1) => Double

  def apply[T1: ClassTag, T2: ClassTag](items: Array[(T1, T2)], distance: Distance[T1], leafSize: Int): VPTree[T1, T2] =
    VPTree(mkNode(items, distance, leafSize), distance)

  sealed trait Tree[T1, T2] {
    def size: Int

    def toArray: Array[(T1, T2)]

    def nearN(t: T1, maxDist: Double, distance: Distance[T1]): Array[(T1, T2)]

    def approxNear(t: T1, f: Distance[T1]): (T1, T2)

    def approxNearN(t: T1, n: Int, f: Distance[T1]): Array[(T1, T2)]
  }

  case class Node[T1, T2](point: (T1, T2), radius: Double, size: Int, in: Tree[T1, T2], out: Tree[T1, T2]) extends Tree[T1, T2] {
    def toArray = in.toArray ++ out.toArray

    def nearN(t: T1, maxDist: Double, distance: Distance[T1]): Array[(T1, T2)] = {
      val d = distance(t, point._1)
      if (d + maxDist < radius) {
        in.nearN(t, maxDist, distance)
      } else if (d - maxDist >= radius) {
        out.nearN(t, maxDist, distance)
      } else {
        in.nearN(t, maxDist, distance) ++ out.nearN(t, maxDist, distance)
      }
    }

    def approxNear(t: T1, f: Distance[T1]): (T1, T2) = {
      val d = f(point._1, t)
      if (d < radius) in.approxNear(t, f)
      else out.approxNear(t, f)
    }

    def approxNearN(t: T1, n: Int, f: Distance[T1]): Array[(T1, T2)] =
      if (n <= 0) Array.empty
      else if (n > size) toArray
      else {
        val d = f(point._1, t)
        if (d < radius) {
          in.approxNearN(t, n, f) ++ out.approxNearN(t, n - in.size, f)
        } else {
          out.approxNearN(t, n, f) ++ in.approxNearN(t, n - out.size, f)
        }
      }
  }

  case class Leaf[T1: ClassTag, T2: ClassTag](points: Array[(T1, T2)]) extends Tree[T1, T2] {
    def size = points.length

    def toArray = points

    def approxNear(t: T1, distance: Distance[T1]): (T1, T2) = points.minBy(p => distance(t, p._1))

    def approxNearN(t: T1, n: Int, distance: Distance[T1]): Array[(T1, T2)] =
      if (n <= 0) Array.empty
      else if (n >= size) points
      else points.sortBy(p => distance(p._1, t)).take(n)

    def nearN(t: T1, maxDist: Double, distance: Distance[T1]): Array[(T1, T2)] =
      points.filter(p => distance(t, p._1) <= maxDist)
  }

  def mkNode[T1: ClassTag, T2: ClassTag](items: Array[(T1, T2)], distance: Distance[T1], leafSize: Int): Tree[T1, T2] = {
    if (items.length <= leafSize)
      Leaf[T1, T2](items)
    else {
      val vp = items(util.Random.nextInt(items.length))

      val radius = {
        val numSamples = math.sqrt(items.length).floor * 2
        val distances = pickSample(items, numSamples.toInt).map(i => distance(vp._1, i._1))
        distances.sortBy(identity).apply(distances.length / 2)
      }

      val (in, out) = items partition (item => distance(item._1, vp._1) < radius)

      if (in.length == 0) Leaf[T1, T2](out)
      else if (out.length == 0) Leaf[T1, T2](in)
      else Node(vp, radius, items.length, mkNode(in, distance, leafSize), mkNode(out, distance, leafSize))
    }
  }

  def pickSample[T1, T2](items: Array[(T1, T2)], size: Int): Array[(T1, T2)] =
    if (items.length <= size) items
    else Array.fill(size)(items(util.Random.nextInt(items.length)))
}
