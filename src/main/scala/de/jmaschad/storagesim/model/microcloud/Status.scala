package de.jmaschad.storagesim.model.microcloud

object Status {
    def apply(buckets: Set[String]) = new Status(buckets)
}

class Status(val buckets: Set[String]) {
    override def toString = "%d buckets".format(buckets.size)
}