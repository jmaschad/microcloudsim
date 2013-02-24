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
import de.jmaschad.storagesim.model.transfer.Upload
import de.jmaschad.storagesim.model.BaseEntity
import de.jmaschad.storagesim.model.transfer.dialogs.RestAck
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.model.transfer.Download
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
    failureBehavior: MicroCloudFailureBehavior,
    distributor: Distributor) extends BaseEntity(name, region) with DialogEntity with ProcessingEntity {

    protected val bandwidth = resourceCharacteristics.bandwidth

    private var storageSystem = new StorageSystem(log _, resourceCharacteristics.storageDevices)
    private var state: MicroCloudState = new OnlineState
    private var firstKill = true

    def initialize(objects: Set[StorageObject]) = storageSystem.addAll(objects)

    override def startEntity(): Unit = {
        anounce(CloudOnline())

        // schedule the next catastrophe
        scheduleKill()
    }

    override def shutdownEntity() =
        log(processing.jobCount + " running jobs on shutdown")

    override def processEvent(event: SimEvent) =
        state.process(event)

    override protected def dialogsEnabled: Boolean =
        state match {
            case _: OnlineState => true
            case _ => false
        }

    override protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] =
        state.createMessageHandler(dialog, content)

    override def toString = "%s %s".format(getClass.getSimpleName, getName)

    private def switchState(newState: MicroCloudState): Unit =
        state = newState

    private def scheduleKill() = if (firstKill) {
        // additionally delay the first kill for a fraction of the MTTF. Otherwise MicroClouds
        // might initially die very soon after another. Also, don't crash during the system boot
        firstKill = false
        val meanTimeToFailure = failureBehavior.cloudFailureDistribution.getNumericalMean()
        val firstKillAdditionalDelay = new UniformRealDistribution(0.0, meanTimeToFailure).sample
        send(getId, failureBehavior.timeToCloudFailure + firstKillAdditionalDelay, MicroCloud.Kill)
    } else {
        send(getId, failureBehavior.timeToCloudFailure, MicroCloud.Kill)
    }

    private def anounce(content: CloudStatusDialog): Unit = {
        val dialog = dialogCenter.openDialog(distributor.getId)
        dialog.messageHandler = content => content match {
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
                scheduleKill()
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
                send(getId, failureBehavior.timeToCloudRepair, MicroCloud.Boot)
                switchState(new OfflineState)

            case _ =>
                MicroCloud.super.processEvent(event)
        }

        def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] =
            content match {
                case restDialog: RestDialog => restDialogHandler(dialog, restDialog)
                case placementDialog: PlacementDialog => placementDialogHandler(dialog, placementDialog)
                case _ =>
                    throw new IllegalStateException("unknown message " + content)
            }

        private def restDialogHandler(dialog: Dialog, content: RestDialog): Option[DialogCenter.MessageHandler] =
            content match {
                case Get(obj) =>
                    val handler: DialogCenter.MessageHandler =
                        content => content match {
                            case Get(obj) =>
                                dialog.say(RestAck, () => dialog.close)

                            case DownloadReady =>
                                val transaction = storageSystem.loadTransaction(obj)
                                new Upload(log _, dialog, obj.size, processing.loadAndUpload(_, transaction, _), (success) => {
                                    dialog.close
                                    if (success) transaction.complete else transaction.abort
                                })

                            case _ => throw new IllegalStateException
                        }
                    Some(handler)

                case _ => throw new IllegalStateException
            }

        private def placementDialogHandler(dialog: Dialog, content: PlacementDialog): Option[DialogCenter.MessageHandler] =
            Some(content => content match {
                case Load(objSourceMap) =>
                    dialog.sayAndClose(PlacementAck)
                    objSourceMap.map(objSrc => openGetDialog(objSrc._2, objSrc._1))

                case _ => throw new IllegalStateException
            })

        private def openGetDialog(target: Int, obj: StorageObject): Unit = {
            val dialog = dialogCenter.openDialog(target)

            dialog.messageHandler = (message) => message match {
                case RestAck =>
                    val transaction = storageSystem.storeTransaction(obj)
                    val onFinish = (success: Boolean) => {
                        dialog.close()
                        transaction.complete
                        anounce(DownloadFinished(obj))
                        if (!success) throw new IllegalStateException
                    }

                    new Download(log _, dialog, obj.size, processing.downloadAndStore(_, transaction, _), onFinish)
                    anounce(DownloadStarted(obj))

                case _ =>
                    throw new IllegalStateException
            }

            dialog.say(Get(obj), () => {
                throw new IllegalStateException
            })
        }
    }
}

