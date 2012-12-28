package de.jmaschad.storagesim.model.behavior

import org.apache.commons.math3.distribution.UniformRealDistribution
import scala.collection.mutable.Map
import de.jmaschad.storagesim.model.storage.StorageObject
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.UserRequest

object Behavior {
  def uniformTimeUniformObject(start: Double, end: Double, rate: Int, objects: IndexedSeq[StorageObject], requestGen: StorageObject => UserRequest): Behavior = {
    val timer = new UniformTimer(start, end)
    val selector = new UniformObjectSelector(objects)
    val requestCount = (end - start) * rate
    new BehaviorImpl(requestCount.longValue, timer, selector, requestGen)
  }
}

trait Behavior {
  def requests(): Map[Double, UserRequest]
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
  requestGen: StorageObject => UserRequest) extends Behavior {
  def requests(): Map[Double, UserRequest] = {
    val reqs = requestTimer.delays(requestCount) zip (objectSelector.storageObjects(requestCount) map requestGen)
    Map(reqs: _*)
  }
}
