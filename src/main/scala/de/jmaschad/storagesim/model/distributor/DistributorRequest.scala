package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.processing.StorageObject
import org.cloudbus.cloudsim.core.SimEvent

object DistributorRequest {
    def fromEvent(event: SimEvent) = event.getData match {
        case req: DistributorRequest => req
        case _ => throw new IllegalStateException
    }
}

abstract sealed class DistributorRequest
case class Load(source: Int, storageObject: StorageObject) extends DistributorRequest
case class Remove(storageObject: StorageObject) extends DistributorRequest
