package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.MicroCloud
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.BaseEntity
import de.jmaschad.storagesim.model.ProcessingEntity
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog
import de.jmaschad.storagesim.model.transfer.dialogs.Result
import de.jmaschad.storagesim.model.transfer.dialogs.Lookup
import de.jmaschad.storagesim.model.transfer.dialogs.CloudLookupDialog
import de.jmaschad.storagesim.model.transfer.dialogs.CloudStatusDialog
import de.jmaschad.storagesim.model.transfer.dialogs.CloudOnline
import de.jmaschad.storagesim.model.transfer.dialogs.CloudStatusAck
import de.jmaschad.storagesim.model.transfer.dialogs.ObjectAdded
import de.jmaschad.storagesim.model.transfer.dialogs.CloudOnline
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.RandomBucketBased
import de.jmaschad.storagesim.RandomObjectBased
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.Dialog
import de.jmaschad.storagesim.PlacementBased
import de.jmaschad.storagesim.DynamicPlacementBased

object Distributor {
    val PeriodicUpdateInterval = 1.0

    val Base = 10100
    val MicroCloudOffline = Base + 1
    val UserRequest = MicroCloudOffline + 1
    val PeriodicUpdate = UserRequest + 1
}
import Distributor._

class Distributor(name: String) extends BaseEntity(name, 0) with DialogEntity {
    private val selector = StorageSim.configuration.selector match {
        case RandomBucketBased() =>
            new RandomBucketBasedSelector(log _, this)

        case RandomObjectBased() =>
            new RandomFileBasedSelector(log _, this)

        case PlacementBased() =>
            new PlacementBasedSelector(log _, this)

        case DynamicPlacementBased() =>
            new DynamicPlacementBasedSelector(log _, this)

        case _ => throw new IllegalStateException
    }

    def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject], users: Set[User]) =
        selector.initialize(initialClouds, initialObjects, users)

    override def startEntity() = {
        super.startEntity

        send(getId, PeriodicUpdateInterval, PeriodicUpdate)
    }

    override def processEvent(event: SimEvent): Unit = event.getTag() match {
        case MicroCloudOffline =>
            selector.removeCloud(event.getSource())

        case PeriodicUpdate =>
            selector.optimizePlacement()
            send(getId, PeriodicUpdateInterval, PeriodicUpdate)

        case _ => super.processEvent(event)
    }

    protected override def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogEntity.MessageHandler] =
        content match {
            case _: CloudLookupDialog => createLookupHandler(dialog)
            case _: CloudStatusDialog => createStatusHandler(dialog)
            case _ => throw new IllegalStateException
        }

    private def createLookupHandler(dialog: Dialog): Option[DialogEntity.MessageHandler] =
        Some((content) => content match {
            case Lookup(Get(obj)) =>
                val entity = Entity.entityForId(dialog.partner)
                selector.selectForGet(entity.netID, obj) match {
                    case cloud if cloud >= 0 =>
                        dialog.sayAndClose(Result(cloud))

                    case _ => throw new IllegalStateException
                }

            case _ => throw new IllegalStateException
        })

    private def createStatusHandler(dialog: Dialog): Option[DialogEntity.MessageHandler] =
        Some((content) => content match {
            case CloudOnline() =>
                selector.addCloud(dialog.partner)
                dialog.sayAndClose(CloudStatusAck())

            case ObjectAdded(obj) =>
                selector.addedObject(dialog.partner, obj)
                dialog.sayAndClose(CloudStatusAck())

            case _ => throw new IllegalStateException
        })

    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())
}