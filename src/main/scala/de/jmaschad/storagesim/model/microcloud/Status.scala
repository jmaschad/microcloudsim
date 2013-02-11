package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.StorageObject

object Status {
    def apply(objects: Set[StorageObject]) = new Status(objects)

    def fromEvent(event: SimEvent) = event.getData match {
        case status: Status => status
        case _ => throw new IllegalStateException
    }
}

class Status(val objects: Set[StorageObject]) {
    override def toString = objects.size + " objects"
}