package de.jmaschad.storagesim.model.behavior

import scala.collection.mutable.Map

import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.apache.commons.math3.distribution.UniformRealDistribution

import de.jmaschad.storagesim.model.request.Request
import de.jmaschad.storagesim.model.storage.StorageObject

object Behavior {
  def uniformTimeUniformObject(start: Double, end: Double, rate: Double, objects: IndexedSeq[StorageObject], requestGen: (StorageObject, Double) => Request): Behavior = {
    val timer = new UniformTimer(start, end)
    val selector = new UniformObjectSelector(objects)
    val requestCount = (end - start) * rate
    new BehaviorImpl(requestCount.longValue, timer, selector, requestGen)
  }
}

trait Behavior {
  def requests(): Map[Double, Request]
}

private[behavior] trait RequestTimer {
  def delays(delayCount: Long): Seq[Double]
}

private[behavior] class UniformTimer(start: Double, end: Double) extends RequestTimer {
  def delays(delayCount: Long): Seq[Double] = {
    val dist = new UniformRealDistribution(start, end)
    for (_ <- 1L to delayCount) yield dist.sample()
  }
}

private[behavior] trait StorageObjectSelector {
  def storageObjects(objectCount: Long): Seq[StorageObject]
}

private[behavior] class UniformObjectSelector(objects: IndexedSeq[StorageObject]) extends StorageObjectSelector {
  override def storageObjects(objectCount: Long): Seq[StorageObject] = {
    val dist = new UniformIntegerDistribution(0, objects.length - 1)
    for (i <- 1L to objectCount) yield objects(dist.sample())
  }
}

private[behavior] class BehaviorImpl(
  requestCount: Long,
  requestTimer: RequestTimer,
  objectSelector: StorageObjectSelector,
  requestGen: (StorageObject, Double) => Request) extends Behavior {
  def requests(): Map[Double, Request] = {
    val reqs = requestTimer.delays(requestCount).zip(objectSelector.storageObjects(requestCount)).map(p => p._1 -> requestGen(p._2, p._1))
    Map(reqs: _*)
  }
}
