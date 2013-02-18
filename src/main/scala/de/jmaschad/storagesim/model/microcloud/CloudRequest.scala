package de.jmaschad.storagesim.model.microcloud

import de.jmaschad.storagesim.model.processing.StorageObject
import org.cloudbus.cloudsim.core.SimEvent

object CloudRequest {
    def fromEvent(event: SimEvent): CloudRequest = event.getData() match {
        case req: CloudRequest => req
        case _ => throw new IllegalStateException
    }
}

abstract sealed class CloudRequest
case class Get(transferId: String, obj: StorageObject) extends CloudRequest
case class Delete(obj: StorageObject) extends CloudRequest
