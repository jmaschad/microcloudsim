package de.jmaschad.storagesim.model.user

import scala.collection.mutable
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.disposer.Disposer

object User {
    private val Base = 10300
    val RequestFailed = Base + 1
    val RequestDone = RequestFailed + 1
    val ScheduleRequest = RequestDone + 1
}
import User._

class User(
    name: String,
    disposer: Disposer) extends SimEntity(name) {

    private val behaviors = scala.collection.mutable.Buffer.empty[Behavior]
    private val log = Log.line("User '%s'".format(getName), _: String)
    private val tracker = new RequestTracker

    def addBehavior(behavior: Behavior) = {
        behaviors += behavior
        if (CloudSim.running()) {
            send(getId, behavior.timeToNextEvent(), ScheduleRequest, behavior)
        }
    }

    override def startEntity(): Unit = {
        // wait two tenth of a second for the system to start up
        behaviors.foreach(b => send(getId, 0.2 + b.timeToNextEvent, ScheduleRequest, b))
    }

    override def shutdownEntity() = log(tracker.summary())

    override def processEvent(event: SimEvent): Unit = event.getTag() match {
        case RequestDone =>
            tracker.completed(getRequest(event))

        case RequestFailed =>
            tracker.failed(getRequest(event))

        case ScheduleRequest =>
            if (CloudSim.clock() <= StorageSim.simDuration) {
                val behavior = event.getData match {
                    case b: Behavior => b
                    case _ => throw new IllegalStateException
                }

                val request = behavior.nextRequest()
                tracker.add(request)

                sendNow(disposer.getId, Disposer.UserRequest, request)
                send(getId, behavior.timeToNextEvent().max(0.001), ScheduleRequest, behavior)
            }

        case _ => log("dropped event" + event)
    }

    private def getRequest(event: SimEvent): Request = {
        event.getData() match {
            case req: Request => req
            case _ => throw new IllegalArgumentException
        }
    }
}

private[user] class RequestTracker {
    var openRequests = Set.empty[Request]
    var completedRequests = Map.empty[Request, Double]
    var failedRequests = Map.empty[Request, Double]

    def add(request: Request) =
        openRequests += request

    def completed(request: Request) = {
        assert(openRequests.contains(request))
        openRequests -= request
        completedRequests += (request -> CloudSim.clock())
    }

    def failed(request: Request) = {
        assert(openRequests.contains(request))
        openRequests -= request
        failedRequests += (request -> CloudSim.clock())
    }

    def summary(): String = {
        "%d completed, %d failed, %d missing".format(completedRequests.size, failedRequests.size, openRequests.size)
    }
}