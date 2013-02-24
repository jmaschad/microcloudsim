package de.jmaschad.storagesim.model.user

import scala.collection.LinearSeq
import org.apache.commons.math3.distribution.IntegerDistribution
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.predicates.PredicateType
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.ResourceCharacteristics
import de.jmaschad.storagesim.model.ProcessingEntity
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.BaseEntity
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.NetDown
import de.jmaschad.storagesim.model.transfer.Dialog
import de.jmaschad.storagesim.model.transfer.Download
import de.jmaschad.storagesim.model.transfer.Dialog
import de.jmaschad.storagesim.model.transfer.Message
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.transfer.dialogs.RestAck
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.transfer.dialogs.Result
import de.jmaschad.storagesim.model.transfer.dialogs.Lookup
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.user.RequestType._

object User {
    private var users = Set.empty[User]
    def allUsers: Set[User] = users

    private val Base = 10300
    val ScheduleRequest = Base + 1
}
import User._

class User(
    name: String,
    region: Int,
    val objects: IndexedSeq[StorageObject],
    val getObjectIndex: IntegerDistribution,
    resources: ResourceCharacteristics,
    distributor: Distributor) extends BaseEntity(name, region) with DialogEntity with ProcessingEntity {
    protected val storageDevices = resources.storageDevices
    protected val bandwidth = resources.bandwidth

    private val behaviors = scala.collection.mutable.Buffer.empty[UserBehavior]
    private val requestLog = new RequestLog(log)

    User.users += this

    def addBehavior(behavior: UserBehavior) = {
        behaviors += behavior
        if (CloudSim.running()) {
            send(getId, behavior.delayModel.sample(), ScheduleRequest, behavior)
        }
    }

    def demand(obj: StorageObject): Double =
        objects.indexOf(obj) match {
            case -1 => 0.0
            case n => getObjectIndex.probability(n)
        }

    override def startEntity(): Unit = {
        behaviors.foreach(b => send(getId, b.delayModel.sample(), ScheduleRequest, b))
    }

    override def shutdownEntity() = log(requestLog.summary())

    override def processEvent(event: SimEvent) =
        event.getTag() match {
            case ScheduleRequest =>
                val reqType = getRequestAndScheduleBehavior(event)
                scheduleRequest(reqType)

            case _ =>
                super.processEvent(event)
        }

    override protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] =
        throw new IllegalStateException

    private def scheduleRequest(requestType: RequestType): Unit = requestType match {
        case RequestType.Get =>
            val obj = objects(getObjectIndex.sample())
            val get = Get(obj)
            requestLog.add(get)
            lookupCloud(get, openGetDialog _)

        case _ => throw new IllegalStateException
    }

    private def lookupCloud[T <: RestDialog](request: T, onSuccess: (Int, T) => Unit): Unit = {
        val dialog = dialogCenter.openDialog(distributor.getId())
        dialog.messageHandler = (message) => message match {
            case Result(cloud) => onSuccess(cloud, request)
            case _ => throw new IllegalStateException
        }
        dialog.say(Lookup(request), () => {
            throw new IllegalStateException
        })
    }

    private def openGetDialog(target: Int, get: Get): Unit = {
        val dialog = dialogCenter.openDialog(target)

        dialog.messageHandler = (message) => message match {
            case RestAck =>
                val onFinish = (success: Boolean) => {
                    dialog.close()
                    val requestState = if (success) Complete else TimeOut

                    requestLog.finish(get, dialog.averageDelay, requestState)
                }

                new Download(log _, dialog, get.obj.size, processing.download _, onFinish)

            case _ =>
                throw new IllegalStateException
        }

        dialog.say(get, () => {
            dialog.close()
            requestLog.finish(get, TimeOut)
        })
    }

    private def getRequestAndScheduleBehavior(event: SimEvent): RequestType = {
        val behav = getBehavior(event)
        val delay = behav.delayModel.sample().max(0.001)
        send(getId, delay, ScheduleRequest, behav)
        behav.requestType
    }

    private def getBehavior(event: SimEvent): UserBehavior =
        event.getData match {
            case b: UserBehavior => b
            case _ => throw new IllegalStateException
        }
}
