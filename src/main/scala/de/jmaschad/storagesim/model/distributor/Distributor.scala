package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.Get
import de.jmaschad.storagesim.model.microcloud.RequestSummary._
import de.jmaschad.storagesim.model.BaseEntity
import de.jmaschad.storagesim.model.ProcessingEntity
import de.jmaschad.storagesim.model.DialogEntity
import Distributor._
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.transfer.Dialog
import de.jmaschad.storagesim.model.transfer.Message
import de.jmaschad.storagesim.model.microcloud.CloudRequest

object Distributor {
    val StatusInterval = 1

    val Base = 10100
    val MicroCloudStatusMessage = Base + 1
    val MicroCloudOnline = MicroCloudStatusMessage + 1
    val MicroCloudOffline = MicroCloudOnline + 1
    val UserRequest = MicroCloudOffline + 1
}

class Distributor(name: String) extends BaseEntity(name) with DialogEntity {
    private val selector = new RandomBucketBasedSelector(log _, sendNow _)

    def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject]) =
        selector.initialize(initialClouds, initialObjects)

    override def processEvent(event: SimEvent): Unit = event.getTag() match {
        case MicroCloudOnline =>
            selector.addCloud(event.getSource(), event.getData())

        case MicroCloudOffline =>
            selector.removeCloud(event.getSource())

        case MicroCloudStatusMessage =>
            selector.processStatusMessage(event.getSource(), event.getData())

        case _ => super.processEvent(event)
    }

    protected override def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] =
        Some((content) => content match {
            case Lookup(Get(obj)) =>
                selector.selectForGet(obj) match {
                    case Right(cloud) =>
                        dialog.say(Result(cloud), () => {})
                        dialog.close()

                    case _ => throw new IllegalStateException
                }

            case _ => throw new IllegalStateException
        })

    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())
}

abstract sealed class DistributorDialog
case class Lookup(request: CloudRequest) extends DistributorDialog
case class Result(cloud: Int) extends DistributorDialog