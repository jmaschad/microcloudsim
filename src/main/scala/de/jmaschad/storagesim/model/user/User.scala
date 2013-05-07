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
    val bandwidth: Double,
    distributor: Distributor) extends BaseEntity(name, region) with DialogEntity with ProcessingEntity {

    private val getInterval = new NormalDistribution(medianGetDelay, 0.1 * medianGetDelay)
    private val requestLog = new RequestLog(log)

    private val objectIndexMap = Random.shuffle(objects.toIndexedSeq) zip (0 until objects.size) toMap
    private val indexObjectMap = objectIndexMap map { case (obj, index) => index -> obj }

    User.users += this

    def demand(obj: StorageObject): Double =
        objectIndexMap.get(obj) match {
            case Some(index) => getObjectIndex.probability(index)
            case None => 0.0
        }

    override def startEntity(): Unit = {
        super.startEntity()
        val firstGetIn = getInterval.sample().max(0.01)
        send(getId, firstGetIn, ScheduleGet)
    }

    override def shutdownEntity() = log(requestLog.summary())

    override def processEvent(event: SimEvent) =
        event.getTag() match {
            case ScheduleGet =>
                scheduleRequest(RequestType.Get)
                send(getId, getInterval.sample().max(0.01), ScheduleGet)

            case _ =>
                super.processEvent(event)
        }

    override protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogEntity.MessageHandler] =
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
                    val requestState = if (success) Complete else TimeOut

                    requestLog.finish(get, dialog.averageDelay, requestState)
                }

                new Downloader(log _, dialog, get.obj.size, download _, onFinish)

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
