package de.jmaschad.storagesim.model.user

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.processing.StorageObject
import org.cloudbus.cloudsim.core.SimEvent

object RequestType extends Enumeration {
    type RequestType = Value

    val Get = Value("GET")
    val Put = Value("PUT")
}
import RequestType._

object Request {
    def get(user: User, storageObject: StorageObject, transferId: String): Request = new Request(user, Get, storageObject, transferId)
    def put(user: User, storageObject: StorageObject, transferId: String): Request = new Request(user, Put, storageObject, transferId)

    def fromEvent(event: SimEvent): Request = event.getData() match {
        case req: Request => req
        case _ => throw new IllegalArgumentException
    }
}

class Request(
    val user: User,
    val requestType: RequestType.Value,
    val storageObject: StorageObject,
    val transferId: String,
    val time: Double = CloudSim.clock()) {

    override def toString = "%s request for %s".format(requestType, storageObject)
}
