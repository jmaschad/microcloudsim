package de.jmaschad.storagesim.model

import de.jmaschad.storagesim.model.transfer.dialogs.CloudOnline
import de.jmaschad.storagesim.model.transfer.dialogs.CloudStatusDialog
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import de.jmaschad.storagesim.model.transfer.dialogs.RestAck
import org.apache.commons.math3.distribution.UniformRealDistribution
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.transfer.dialogs.CloudStatusAck
import de.jmaschad.storagesim.model.transfer.Uploader
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.model.transfer.dialogs.DownloadReady
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.RealDistributionConfiguration
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog
import de.jmaschad.storagesim.model.transfer.Downloader
import de.jmaschad.storagesim.StorageSim
import MicroCloud._
import de.jmaschad.storagesim.model.transfer.dialogs.DownloadReady
import de.jmaschad.storagesim.model.transfer.dialogs.ObjectAdded

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
    val bandwidth: Double,
    distributor: Distributor) extends BaseEntity(name, region) with DialogEntity with ProcessingEntity {

    //    private val meanTimeToFailure = RealDistributionConfiguration.toDist(StorageSim.configuration.meanTimeToFailure)

    private var state: MicroCloudState = new OnlineState

    var objects = Set.empty[StorageObject]

    def initialize(objs: Set[StorageObject]) =
        objects = objs

    def isEmpty =
        objects.isEmpty

    def contains(obj: StorageObject) =
        objects.contains(obj)

    override def startEntity(): Unit = {
        super.startEntity()
        anounce(CloudOnline())

        //        val uptime = new UniformRealDistribution(0.0, meanTimeToFailure.getNumericalMean()).sample()
        //        scheduleFailure(uptime)
    }

    override def processEvent(event: SimEvent) =
        state.process(event)

    override protected def dialogsEnabled: Boolean =
        state.isInstanceOf[OnlineState]

    override protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogEntity.MessageHandler] =
        state.createMessageHandler(dialog, content)

    override def toString = "%s %s".format(getClass.getSimpleName, getName)

    //    private def scheduleFailure(uptime: Double = 0.0) = {
    //        var ttf = -uptime
    //        while (ttf <= 0.0) {
    //            ttf += meanTimeToFailure.sample()
    //        }
    //
    //        schedule(getId(), ttf, Kill)
    //    }

    //    private def scheduleReplace() = {
    //        // 30 minutes until replacement
    //        schedule(getId(), 30 * 60, Boot)
    //    }

    private def switchState(newState: MicroCloudState): Unit =
        state = newState

    private def anounce(content: CloudStatusDialog): Unit = {
        val dialog = openDialog(distributor.getId)
        dialog.messageHandler = {
            case CloudStatusAck() => dialog.close
            case _ => throw new IllegalStateException
        }
        dialog.say(content, () => { throw new IllegalStateException })
    }

    private trait MicroCloudState {
        def process(event: SimEvent)
        def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogEntity.MessageHandler]
    }

    private class OfflineState extends MicroCloudState {
        def process(event: SimEvent) = event.getTag match {
            case MicroCloud.Boot =>
                log("received boot request")
                //                scheduleFailure()
                anounce(CloudOnline())
                switchState(new OnlineState)

            case _ =>
                MicroCloud.super.processEvent(event)
        }

        def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogEntity.MessageHandler] = {
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
                objects = Set.empty
                reset()
                //                scheduleReplace()
                sendNow(distributor.getId, Distributor.MicroCloudOffline)
                switchState(new OfflineState)

            case _ =>
                MicroCloud.super.processEvent(event)
        }

        def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogEntity.MessageHandler] =
            content match {
                case restDialog: RestDialog =>
                    restDialogHandler(dialog, restDialog)

                case placementDialog: PlacementDialog =>
                    placementDialogHandler(dialog, placementDialog)

                case _ =>
                    throw new IllegalStateException("unknown message " + content)
            }

        private def restDialogHandler(dialog: Dialog, content: RestDialog): Option[DialogEntity.MessageHandler] =
            content match {
                case Get(obj) =>
                    Some({
                        case Get(obj) =>
                            dialog.say(RestAck, () => dialog.close)

                        case DownloadReady =>
                            new Uploader(log _, dialog, obj.size, upload(_, _, _), _ => dialog.close)

                        case _ => throw new IllegalStateException
                    })

                case _ => throw new IllegalStateException
            }

        private def placementDialogHandler(dialog: Dialog, content: PlacementDialog): Option[DialogEntity.MessageHandler] =
            Some({
                case Load(objSourceMap) =>
                    dialog.sayAndClose(PlacementAck)
                    objSourceMap map { case (obj, cloudID) => openGetDialog(cloudID, obj) }

                case _ => throw new IllegalStateException
            })

        private def openGetDialog(target: Int, obj: StorageObject): Unit = {
            val dialog = openDialog(target)

            dialog.messageHandler = {
                case RestAck =>
                    new Downloader(log _, dialog, obj.size, download(_, _, _), { success =>
                        dialog.close()
                        if (success) {
                            assert(!objects.contains(obj))
                            objects += obj
                            anounce(ObjectAdded(obj))
                        }
                    })

                case _ =>
                    throw new IllegalStateException
            }

            dialog.say(Get(obj), () => throw new IllegalStateException)
        }
    }
}

