package de.jmaschad.storagesim.model.request

import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.User
import org.cloudbus.cloudsim.core.CloudSim

object RequestType extends Enumeration {
    val Get = Value("GET")
    val Put = Value("PUT")
}
import RequestType._

object Request {
    def get(user: User, storageObject: StorageObject): Request = new Request(user, Get, storageObject, CloudSim.clock()) {}
    def put(user: User, storageObject: StorageObject): Request = new Request(user, Put, storageObject, CloudSim.clock()) {}
}

abstract class Request(
    val user: User,
    val requestType: RequestType.Value,
    val storageObject: StorageObject,
    val time: Double) {

    override def toString = "%s request for %s".format(requestType, storageObject)
}
