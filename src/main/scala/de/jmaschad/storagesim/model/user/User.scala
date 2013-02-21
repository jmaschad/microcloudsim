package de.jmaschad.storagesim.model.user

import scala.collection.LinearSeq
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.predicates.PredicateType
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.ResourceCharacteristics
import de.jmaschad.storagesim.model.ProcessingEntity
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.NetDown
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.CloudRequest
import de.jmaschad.storagesim.model.microcloud.Get
import de.jmaschad.storagesim.model.transfer.Dialog
import de.jmaschad.storagesim.model.microcloud.RequestAck
import de.jmaschad.storagesim.model.transfer.Download
import de.jmaschad.storagesim.model.transfer.Dialog
import de.jmaschad.storagesim.model.microcloud.RequestSummary._
import User._
import de.jmaschad.storagesim.model.transfer.Message
import de.jmaschad.storagesim.model.transfer.DialogCenter

object User {
    private val MaxRequestTicks = 2

    private val Base = 10300
    val ScheduleRequest = Base + 1
}

class User(
    name: String,
    resources: ResourceCharacteristics,
    initialObjects: Iterable[StorageObject],
    distributor: Distributor) extends ProcessingEntity(name, resources) {
    private val behaviors = scala.collection.mutable.Buffer.empty[UserBehavior]
    private val requestLog = new RequestLog(log)

    private var openRequests = Set.empty[CloudRequest]

    def addBehavior(behavior: UserBehavior) = {
        behaviors += behavior
        if (CloudSim.running()) {
            send(getId, behavior.timeToNextEvent(), ScheduleRequest, behavior)
        }
    }

    override def log(msg: String) = Log.line("User '%s'".format(getName), msg: String)

    override def startEntity(): Unit = {
        behaviors.foreach(b => send(getId, b.timeToNextEvent, ScheduleRequest, b))
    }

    override def shutdownEntity() = log(requestLog.summary())

    override def process(event: SimEvent) =
        event.getTag() match {
            case ScheduleRequest =>
                val request = getRequestAndScheduleBehavior(event)
                sendRequest(request)

            case _ =>
                log("dropoped event: " + event)
        }

    override protected def createMessageHandler(dialog: Dialog, message: Message): Option[DialogCenter.MessageHandler] =
        throw new IllegalStateException

    private def sendRequest(request: CloudRequest) =
        request match {
            case get @ Get(obj) =>
                requestLog.add(get)
                distributor.cloudForGet(get) match {
                    case Right(cloud) =>
                        sendGet(cloud, get)

                    case Left(error) =>
                        requestLog.finish(get, error)
                }

            case _ => throw new IllegalStateException
        }

    private def sendGet(target: Int, get: Get) = {
        val dialog = dialogCenter.openDialog(target)

        dialog.messageHandler = (message) => message.content match {
            case RequestAck(req) if req == get =>
                val onFinish = (success: Boolean) => {
                    dialogCenter.closeDialog(dialog)

                    if (success)
                        requestLog.finish(get, Complete)
                    else
                        requestLog.finish(get, TimeOut)
                }

                new Download(log _, dialog, get.obj.size, processing.download _, onFinish)

            case _ =>
                throw new IllegalStateException
        }

        dialog.say(get, () => requestLog.finish(get, TimeOut))
    }

    private def getRequestAndScheduleBehavior(event: SimEvent): CloudRequest = {
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
