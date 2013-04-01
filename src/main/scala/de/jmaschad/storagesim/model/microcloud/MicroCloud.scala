package de.jmaschad.storagesim.model.microcloud

import scala.Enumeration
import org.apache.commons.math3.distribution.UniformRealDistribution
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.ProcessingEntity
import de.jmaschad.storagesim.model.ResourceCharacteristics
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.transfer.Dialog
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.transfer.Message
import de.jmaschad.storagesim.model.transfer.Uploader
import de.jmaschad.storagesim.model.BaseEntity
import de.jmaschad.storagesim.model.transfer.dialogs.RestAck
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.model.transfer.Downloader
import de.jmaschad.storagesim.model.transfer.dialogs.CloudStatusAck
import de.jmaschad.storagesim.model.transfer.dialogs.CloudOnline
import de.jmaschad.storagesim.model.transfer.dialogs.CloudStatusDialog
import de.jmaschad.storagesim.model.transfer.dialogs.DownloadStarted
import de.jmaschad.storagesim.model.transfer.dialogs.DownloadFinished
import de.jmaschad.storagesim.model.transfer.dialogs.DownloadReady

object MicroCloud {
    private val Base = 10200

    val Initialize = Base + 1
    val Boot = Initialize + 1
    val Shutdown = Boot + 1
    val Kill = Shutdown + 1
    val Request = Kill + 1
    val DistributorRequest = Request + 1
}

class MicroCloud(
    name: String,
    region: Int,
    resourceCharacteristics: ResourceCharacteristics,
    distributor: Distributor) extends BaseEntity(name, region) with DialogEntity with ProcessingEntity {

    protected val bandwidth = resourceCharacteristics.bandwidth

    private var storageSystem = new StorageSystem(log _, resourceCharacteristics.storageDevices)
    private var state: MicroCloudState = new OnlineState
    private var firstKill = true

    def initialize(objects: Set[StorageObject]) = storageSystem.addAll(objects)

    def isEmpty = storageSystem.isEmpty

    override def startEntity(): Unit = {
        anounce(CloudOnline())
    }

    override def shutdownEntity() =
        log(processing.jobCount + " running jobs on shutdown")

    override def processEvent(event: SimEvent) =
        state.process(event)

    override protected def dialogsEnabled: Boolean =
        state.isInstanceOf[OnlineState]

    override protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] =
        state.createMessageHandler(dialog, content)

    override def toString = "%s %s".format(getClass.getSimpleName, getName)

    private def switchState(newState: MicroCloudState): Unit =
        state = newState

    private def anounce(content: CloudStatusDialog): Unit = {
        val dialog = dialogCenter.openDialog(distributor.getId)
        dialog.messageHandler = {
            case CloudStatusAck() => dialog.close
            case _ => throw new IllegalStateException
        }
        dialog.say(content, () => { throw new IllegalStateException })
    }

    private trait MicroCloudState {
        def process(event: SimEvent)
        def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler]
    }

    private class OfflineState extends MicroCloudState {
        def process(event: SimEvent) = event.getTag match {
            case MicroCloud.Boot =>
                log("received boot request")
                anounce(CloudOnline())
                switchState(new OnlineState)

            case _ =>
                MicroCloud.super.processEvent(event)
        }

        def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] = {
            log("ignored dialog because it is offline")
            None
        }
    }

    private class OnlineState extends MicroCloudState {
        private var dueReplicationAcks = scala.collection.mutable.Map.empty[String, Seq[StorageObject]]

        def process(event: SimEvent) = event.getTag() match {
            case MicroCloud.Shutdown =>
                log("received shutdown request")
                sendNow(distributor.getId(), Distributor.MicroCloudOffline)
                switchState(new OfflineState)

            case MicroCloud.Kill =>
                log("received kill request")
                reset()
                sendNow(distributor.getId, Distributor.MicroCloudOffline)
                switchState(new OfflineState)

            case _ =>
                MicroCloud.super.processEvent(event)
        }

        def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] =
            content match {
                case restDialog: RestDialog =>
                    restDialogHandler(dialog, restDialog)

                case placementDialog: PlacementDialog =>
                    placementDialogHandler(dialog, placementDialog)

                case _ =>
                    throw new IllegalStateException("unknown message " + content)
            }

        private def restDialogHandler(dialog: Dialog, content: RestDialog): Option[DialogCenter.MessageHandler] =
            content match {
                case Get(obj) =>
                    Some({
                        case Get(obj) =>
                            dialog.say(RestAck, () => dialog.close)

                        case DownloadReady =>
                            new Uploader(log _, dialog, obj.size, processing.upload(_, _), _ => dialog.close)

                        case _ => throw new IllegalStateException
                    })

                case _ => throw new IllegalStateException
            }

        private def placementDialogHandler(dialog: Dialog, content: PlacementDialog): Option[DialogCenter.MessageHandler] =
            Some({
                case Load(objSourceMap) =>
                    dialog.sayAndClose(PlacementAck)
                    objSourceMap map { case (obj, cloudID) => openGetDialog(cloudID, obj) }

                case _ => throw new IllegalStateException
            })

        private def openGetDialog(target: Int, obj: StorageObject): Unit = {
            val dialog = dialogCenter.openDialog(target)

            dialog.messageHandler = {
                case RestAck =>
                    val onFinish = { success: Boolean =>
                        dialog.close()
                        anounce(DownloadFinished(obj))
                        if (success) storageSystem.add(obj) else throw new IllegalStateException
                    }

                    new Downloader(log _, dialog, obj.size, processing.download(_, _), onFinish)
                    anounce(DownloadStarted(obj))

                case _ =>
                    throw new IllegalStateException
            }

            dialog.say(Get(obj), () => throw new IllegalStateException)
        }
    }
}

