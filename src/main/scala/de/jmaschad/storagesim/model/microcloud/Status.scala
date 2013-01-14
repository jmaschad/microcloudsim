package de.jmaschad.storagesim.model.microcloud

import scala.collection.mutable

object Status {
  def apply(buckets: mutable.Set[String]) = new Status(buckets)
}

class Status(val buckets: mutable.Set[String]) {
  override def toString = "%d buckets".format(buckets.size)
}