package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.SimEvent

object Status {
    def apply(buckets: Set[String]) = new Status(buckets)

    def fromEvent(event: SimEvent) = event.getData match {
        case status: Status => status
        case _ => throw new IllegalStateException
    }
}

class Status(val buckets: Set[String]) {
    override def toString = "%d buckets".format(buckets.size)
}