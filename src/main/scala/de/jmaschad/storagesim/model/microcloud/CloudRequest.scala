package de.jmaschad.storagesim.model.microcloud

import de.jmaschad.storagesim.model.processing.StorageObject
import org.cloudbus.cloudsim.core.SimEvent

object RequestSummary extends Enumeration {
    type RequestSummary = Value

    val Complete = Value("completed")
    val TimeOut = Value("timed out")
    val ObjectNotFound = Value("object not found")
    val ObjectExists = Value("object exists")
    val UnsufficientSpace = Value("unsufficient space")
    val CloudStorageError = Value("cloud storage error")
}
import RequestSummary._

object CloudRequest {
    def fromEvent(event: SimEvent): CloudRequest = event.getData() match {
        case req: CloudRequest => req
        case _ => throw new IllegalStateException
    }

    var requestId = 0L
    def nextId: Long = {
        val id = requestId
        requestId += 1
        id
    }
}

abstract sealed class CloudRequest(val id: Long = CloudRequest.nextId)
case class Get(obj: StorageObject) extends CloudRequest
case class Delete(obj: StorageObject) extends CloudRequest
case class RequestAck(request: CloudRequest) extends CloudRequest