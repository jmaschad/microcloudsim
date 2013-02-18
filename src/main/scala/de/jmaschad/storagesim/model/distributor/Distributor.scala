package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.user.RequestState
import de.jmaschad.storagesim.model.user.RequestState._
import Distributor._
import de.jmaschad.storagesim.model.microcloud.Get

object Distributor {
    val StatusInterval = 1

    val Base = 10100
    val MicroCloudStatusMessage = Base + 1
    val MicroCloudOnline = MicroCloudStatusMessage + 1
    val MicroCloudOffline = MicroCloudOnline + 1
    val UserRequest = MicroCloudOffline + 1
}

class Distributor(name: String) extends SimEntity(name) {
    private val selector = new RandomBucketBasedSelector(log _, sendNow _)

    def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject]) =
        selector.initialize(initialClouds, initialObjects)

    def cloudForGet(get: Get): Either[RequestState, Int] =
        selector.selectForGet(get.obj)

    override def startEntity(): Unit = {}
    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent): Unit = event.getTag() match {
        case MicroCloudOnline =>
            selector.addCloud(event.getSource(), event.getData())

        case MicroCloudOffline =>
            selector.removeCloud(event.getSource())

        case MicroCloudStatusMessage =>
            selector.processStatusMessage(event.getSource(), event.getData())

        case _ => log("[online] dropped event " + event)
    }

    private def log(msg: String) = Log.line("Distributor '%s'".format(getName), msg: String)
    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())
}
