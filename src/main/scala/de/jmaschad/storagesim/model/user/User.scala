package de.jmaschad.storagesim.model.user

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent

import User.RequestDone
import User.RequestFailed
import User.ScheduleRequest
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.Log

object User {
    private val Base = 10300
    val RequestFailed = Base + 1
    val RequestDone = RequestFailed + 1
    val ScheduleRequest = RequestDone + 1
}

class User(
    name: String,
    disposer: Distributor) extends SimEntity(name) {

    private val behaviors = scala.collection.mutable.Buffer.empty[UserBehavior]
    private val log = Log.line("User '%s'".format(getName), _: String)
    private val tracker = new RequestTracker(log)

    def addBehavior(behavior: UserBehavior) = {
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
            val request = getRequest(event)
            tracker.completed(request)

        case RequestFailed =>
            val request = getRequest(event)
            tracker.failed(getRequest(event))

        case ScheduleRequest =>
            val behavior = event.getData match {
                case b: UserBehavior => b
                case _ => throw new IllegalStateException
            }

            val request = behavior.nextRequest()
            tracker.add(request)

            sendNow(disposer.getId, Distributor.UserRequest, request)
            send(getId, behavior.timeToNextEvent().max(0.001), ScheduleRequest, behavior)

        case _ => log("dropped event" + event)
    }

    private def getRequest(event: SimEvent): Request = {
        event.getData() match {
            case req: Request => req
            case _ => throw new IllegalArgumentException
        }
    }
}

private[user] class RequestTracker(log: String => Unit) {
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