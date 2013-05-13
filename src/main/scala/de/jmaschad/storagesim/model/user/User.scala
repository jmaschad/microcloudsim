package de.jmaschad.storagesim.model.user

import org.apache.commons.math3.distribution.IntegerDistribution
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import RequestType._
import User._
import de.jmaschad.storagesim.model.BaseEntity
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.ProcessingEntity
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import de.jmaschad.storagesim.model.transfer.dialogs.Lookup
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.transfer.dialogs.RestAck
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog
import de.jmaschad.storagesim.model.transfer.dialogs.Result
import org.apache.commons.math3.distribution.NormalDistribution
import de.jmaschad.storagesim.model.Dialog
import scala.util.Random
import de.jmaschad.storagesim.model.transfer.Downloader
import de.jmaschad.storagesim.StatsCentral
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.UniformRealDist
import org.apache.commons.math3.distribution.UniformRealDistribution
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.cloudbus.cloudsim.NetworkTopology
import de.jmaschad.storagesim.model.Entity

object User {
    val GenerateGetInterval = 1.0
    private val Base = 10300
    val ScheduleGet = Base + 1
}

class User(
    name: String,
    netID: Int,
    val objects: Seq[StorageObject],
    var bandwidth: Double,
    distributor: Distributor) extends BaseEntity(name, netID) with DialogEntity with ProcessingEntity {

    private val objectSelection = new UniformIntegerDistribution(0, objects.size - 1)

    override def startEntity(): Unit = {
        super.startEntity()
        send(getId, 0.5, ScheduleGet)
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent) =
        event.getTag() match {
            case ScheduleGet =>
                if (processingModel.loadDown.values.sum < (0.9 * bandwidth)) {
                    val obj = objects(objectSelection.sample())
                    lookupCloud(Get(obj), openGetDialog _)
                }
                send(getId, GenerateGetInterval, ScheduleGet)

            case _ =>
                super.processEvent(event)
        }

    override protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogEntity.MessageHandler] =
        throw new IllegalStateException

    private def lookupCloud[T <: RestDialog](request: T, onSuccess: (Int, T) => Unit): Unit = {
        val dialog = openDialog(distributor.getId())
        dialog.messageHandler = {
            case Result(cloud) => onSuccess(cloud, request)
            case _ => throw new IllegalStateException
        }
        dialog.say(Lookup(request), () => throw new IllegalStateException)
    }

    private def openGetDialog(target: Int, get: Get): Unit = {
        val dialog = openDialog(target)

        dialog.messageHandler = {
            case RestAck =>
                val onFinish = (success: Boolean) => {
                    dialog.close()

                    if (success) {
                        val ownPos = NetworkTopology.getPosition(netID)
                        val targetPos = NetworkTopology.getPosition(Entity.entityForId(target).netID)
                        StatsCentral.requestCompleted(dialog.averageDelay, ownPos.distance(targetPos))
                    }
                }

                new Downloader(log _, dialog, get.obj, download _, onFinish)

            case _ =>
                throw new IllegalStateException
        }

        dialog.say(get, { () =>
            dialog.close()
        })
    }

    private def getBehavior(event: SimEvent): UserBehavior =
        event.getData match {
            case b: UserBehavior => b
            case _ => throw new IllegalStateException
        }
}
