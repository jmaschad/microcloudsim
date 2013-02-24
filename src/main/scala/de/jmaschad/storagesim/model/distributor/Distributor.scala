package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.BaseEntity
import de.jmaschad.storagesim.model.ProcessingEntity
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.transfer.Dialog
import de.jmaschad.storagesim.model.transfer.Message
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog
import de.jmaschad.storagesim.model.transfer.dialogs.Result
import de.jmaschad.storagesim.model.transfer.dialogs.Lookup
import de.jmaschad.storagesim.model.transfer.dialogs.CloudLookupDialog
import de.jmaschad.storagesim.model.transfer.dialogs.CloudStatusDialog
import de.jmaschad.storagesim.model.transfer.dialogs.CloudOnline
import de.jmaschad.storagesim.model.transfer.dialogs.CloudStatusAck
import de.jmaschad.storagesim.model.transfer.dialogs.DownloadFinished
import de.jmaschad.storagesim.model.transfer.dialogs.DownloadStarted
import Distributor._
import de.jmaschad.storagesim.model.transfer.dialogs.CloudOnline

object Distributor {
    val StatusInterval = 1

    val Base = 10100
    val MicroCloudOffline = Base + 1
    val UserRequest = MicroCloudOffline + 1
}

class Distributor(name: String) extends BaseEntity(name, 0) with DialogEntity {
    private val selector = new RandomBucketBasedSelector(log _, dialogCenter)

    def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject]) =
        selector.initialize(initialClouds, initialObjects)

    override def processEvent(event: SimEvent): Unit = event.getTag() match {
        case MicroCloudOffline =>
            selector.removeCloud(event.getSource())

        case _ => super.processEvent(event)
    }

    protected override def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] =
        content match {
            case _: CloudLookupDialog => createLookupHandler(dialog)
            case _: CloudStatusDialog => createStatusHandler(dialog)
            case _ => throw new IllegalStateException
        }

    private def createLookupHandler(dialog: Dialog): Option[DialogCenter.MessageHandler] =
        Some((content) => content match {
            case Lookup(Get(obj)) =>
                selector.selectForGet(obj) match {
                    case Right(cloud) =>
                        dialog.sayAndClose(Result(cloud))

                    case _ => throw new IllegalStateException
                }

            case _ => throw new IllegalStateException
        })

    private def createStatusHandler(dialog: Dialog): Option[DialogCenter.MessageHandler] =
        Some((content) => content match {
            case CloudOnline() =>
                selector.addCloud(dialog.partner)
                dialog.sayAndClose(CloudStatusAck())

            case DownloadStarted(obj) =>
                selector.startedDownload(dialog.partner, obj)
                dialog.sayAndClose(CloudStatusAck())

            case DownloadFinished(obj) =>
                selector.finishedDownload(dialog.partner, obj)
                dialog.sayAndClose(CloudStatusAck())

            case _ => throw new IllegalStateException
        })

    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())
}