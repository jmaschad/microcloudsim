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
import de.jmaschad.storagesim.model.transfer.Dialog
import de.jmaschad.storagesim.model.transfer.Downloader
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
import User._
import scala.util.Random
import org.apache.commons.math3.distribution.NormalDistribution

object User {
    private var users = Set.empty[User]
    def allUsers: Set[User] = users

    private val Base = 10300
    val ScheduleGet = Base + 1
}

class User(
    name: String,
    region: Int,
    val objects: Set[StorageObject],
    val getObjectIndex: IntegerDistribution,
    val medianGetDelay: Double,
    resources: ResourceCharacteristics,
    distributor: Distributor) extends BaseEntity(name, region) with DialogEntity with ProcessingEntity {
    protected val storageDevices = resources.storageDevices
    protected val bandwidth = resources.bandwidth

    private val getDelay = new NormalDistribution(medianGetDelay, 0.1 * medianGetDelay)
    private val requestLog = new RequestLog(log)

    private val objectIndexMap = Random.shuffle(objects.toIndexedSeq).zip(0 until objects.size).toMap
    private val indexObjectMap = objectIndexMap map { case (obj, index) => index -> obj }

    User.users += this

    def demand(obj: StorageObject): Double =
        objectIndexMap.get(obj) match {
            case Some(index) => getObjectIndex.probability(index)
            case None => 0.0
        }

    override def startEntity(): Unit = {
        send(getId, getDelay.sample().max(0.01), ScheduleGet)
    }

    override def shutdownEntity() = log(requestLog.summary())

    override def processEvent(event: SimEvent) =
        event.getTag() match {
            case ScheduleGet =>
                scheduleRequest(RequestType.Get)
                send(getId, getDelay.sample().max(0.01), ScheduleGet)

            case _ =>
                super.processEvent(event)
        }

    override protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] =
        throw new IllegalStateException

    private def scheduleRequest(requestType: RequestType): Unit = requestType match {
        case RequestType.Get =>
            val obj = indexObjectMap(getObjectIndex.sample())
            val get = Get(obj)
            requestLog.add(get)
            lookupCloud(get, openGetDialog _)

        case _ => throw new IllegalStateException
    }

    private def lookupCloud[T <: RestDialog](request: T, onSuccess: (Int, T) => Unit): Unit = {
        val dialog = dialogCenter.openDialog(distributor.getId())
        dialog.messageHandler = {
            case Result(cloud) => onSuccess(cloud, request)
            case _ => throw new IllegalStateException
        }
        dialog.say(Lookup(request), () => throw new IllegalStateException)
    }

    private def openGetDialog(target: Int, get: Get): Unit = {
        val dialog = dialogCenter.openDialog(target)

        dialog.messageHandler = {
            case RestAck =>
                val onFinish = (success: Boolean) => {
                    dialog.close()
                    val requestState = if (success) Complete else TimeOut

                    requestLog.finish(get, dialog.averageDelay, requestState)
                }

                new Downloader(log _, dialog, get.obj.size, processing.download _, onFinish)

            case _ =>
                throw new IllegalStateException
        }

        dialog.say(get, { () =>
            dialog.close()
            requestLog.finish(get, TimeOut)
        })
    }

    private def getBehavior(event: SimEvent): UserBehavior =
        event.getData match {
            case b: UserBehavior => b
            case _ => throw new IllegalStateException
        }
}
