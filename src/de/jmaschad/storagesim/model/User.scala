package de.jmaschad.storagesim.model

import scala.collection.mutable
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import User._
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.model.request.GetRequest
import de.jmaschad.storagesim.model.request.PutRequest
import de.jmaschad.storagesim.model.request.Request
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.StorageSim

object User {
    private val Base = 10300
    val RequestFailed = Base + 1
    val RequestDone = RequestFailed + 1
    val ScheduleRequest = RequestDone + 1
}

class User(
    name: String,
    disposer: Disposer) extends SimEntity(name) {

    private val behaviors = scala.collection.mutable.Buffer.empty[Behavior]
    private val log = Log.line("User '%s'".format(getName), _: String)

    def addBehavior(behavior: Behavior) = {
        behaviors += behavior
        if (CloudSim.running()) {
            send(getId, behavior.timeToNextEvent(), ScheduleRequest, behavior)
        }
    }

    override def startEntity(): Unit = {
        behaviors.foreach(b => send(getId, b.timeToNextEvent(), ScheduleRequest, b))
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent): Unit = event.getTag() match {
        case RequestDone =>
            done(event)

        case RequestFailed =>
            failed(event)

        case ScheduleRequest =>
            if (CloudSim.clock() <= StorageSim.simDuration) {
                val behavior = event.getData match {
                    case b: Behavior => b
                    case _ => throw new IllegalStateException
                }

                sendNow(disposer.getId, Disposer.UserRequest, behavior.nextRequest(CloudSim.clock()))
                send(getId, behavior.timeToNextEvent().max(0.001), ScheduleRequest, behavior)
            }

        case _ => log("dropped event" + event)
    }

    private def logReq(req: Request, success: Boolean) = {
        log("%s %s in %.3fs".format(if (success) "SUCCSESS" else "FAILED", req, CloudSim.clock() - req.time))
    }

    private def done(event: SimEvent) = {
        event.getData() match {
            case req: GetRequest => logReq(req, true)
            case req: PutRequest => logReq(req, true)
            case _ => throw new IllegalArgumentException
        }
    }

    private def failed(event: SimEvent) = {
        event.getData() match {
            case req: GetRequest => logReq(req, false)
            case req: PutRequest => logReq(req, false)
            case _ => throw new IllegalArgumentException
        }
    }
}