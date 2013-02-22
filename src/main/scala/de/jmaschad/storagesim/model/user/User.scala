package de.jmaschad.storagesim.model.user

import scala.collection.LinearSeq
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
import de.jmaschad.storagesim.model.transfer.dialogs.RequestAck
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.transfer.dialogs.Result
import de.jmaschad.storagesim.model.transfer.dialogs.Lookup

object User {
    private val MaxRequestTicks = 2

    private val Base = 10300
    val ScheduleRequest = Base + 1
}
import User._

class User(
    name: String,
    resources: ResourceCharacteristics,
    distributor: Distributor) extends BaseEntity(name) with DialogEntity with ProcessingEntity {
    protected val storageDevices = resources.storageDevices
    protected val bandwidth = resources.bandwidth

    private val behaviors = scala.collection.mutable.Buffer.empty[UserBehavior]
    private val requestLog = new RequestLog(log)

    def addBehavior(behavior: UserBehavior) = {
        behaviors += behavior
        if (CloudSim.running()) {
            send(getId, behavior.timeToNextEvent(), ScheduleRequest, behavior)
        }
    }

    override def startEntity(): Unit = {
        behaviors.foreach(b => send(getId, b.timeToNextEvent, ScheduleRequest, b))
    }

    override def shutdownEntity() = log(requestLog.summary())

    override def processEvent(event: SimEvent) =
        event.getTag() match {
            case ScheduleRequest =>
                val request = getRequestAndScheduleBehavior(event)
                sendRequest(request)

            case _ =>
                super.processEvent(event)
        }

    override protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler] =
        throw new IllegalStateException

    private def sendRequest(request: RestDialog) =
        request match {
            case get @ Get(obj) =>
                requestLog.add(get)
                lookupCloud(get, sendGet _)

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

    private def sendGet(target: Int, get: Get): Unit = {
        val dialog = dialogCenter.openDialog(target)

        dialog.messageHandler = (message) => message match {
            case RequestAck(req) if req == get =>
                val onFinish = (success: Boolean) => {
                    dialog.close()
                    if (success)
                        requestLog.finish(get, Complete)
                    else
                        requestLog.finish(get, TimeOut)
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

    private def getRequestAndScheduleBehavior(event: SimEvent): RestDialog = {
        val behav = getBehavior(event)
        val delay = behav.timeToNextEvent().max(0.001)
        send(getId, delay, ScheduleRequest, behav)
        behav.nextRequest
    }

    private def getBehavior(event: SimEvent): UserBehavior =
        event.getData match {
            case b: UserBehavior => b
            case _ => throw new IllegalStateException
        }
}
