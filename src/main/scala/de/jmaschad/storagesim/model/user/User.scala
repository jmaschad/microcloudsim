package de.jmaschad.storagesim.model.user

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
import de.jmaschad.storagesim.model.processing.Download
import de.jmaschad.storagesim.util.Ticker

object User {
    private val MaxRequestTicks = 2

    private val Base = 10300
    val RequestAck = Base + 1
    val RequestRst = RequestAck + 1
    val RequestFailed = RequestRst + 1
    val ScheduleRequest = RequestFailed + 1
}
import User._

class User(
    name: String,
    resources: ResourceCharacteristics,
    initialObjects: Iterable[StorageObject],
    disposer: Distributor) extends ProcessingEntity(name, resources, initialObjects) {
    private val behaviors = scala.collection.mutable.Buffer.empty[UserBehavior]
    private val requestLog = new RequestLog(log)

    private var openRequests = Set.empty[Request]

    def addBehavior(behavior: UserBehavior) = {
        behaviors += behavior
        if (CloudSim.running()) {
            send(getId, behavior.timeToNextEvent(), ScheduleRequest, behavior)
        }
    }

    def scheduleProcessingUpdate(delay: Double) = {
        CloudSim.cancelAll(getId(), new PredicateType(ProcessingModel.ProcUpdate))
        send(getId(), delay, ProcessingModel.ProcUpdate)
    }

    override def log(msg: String) = Log.line("User '%s'".format(getName), msg: String)

    override def startEntity(): Unit = {
        // wait two tenth of a second for the system to start up
        behaviors.foreach(b => send(getId, 0.2 + b.timeToNextEvent, ScheduleRequest, b))
    }

    override def shutdownEntity() = log(requestLog.summary())

    override def process(event: SimEvent): Boolean = {
        event.getTag() match {
            case RequestAck =>
                val request = Request.fromEvent(event)
                assert(openRequests.contains(request))
                openRequests -= request
                processRequest(event.getSource(), request)
                true

            case RequestRst =>
                val request = Request.fromEvent(event)
                assert(openRequests.contains(request))
                openRequests -= request
                requestLog.failed(request)
                true

            case RequestFailed =>
                requestLog.failed(Request.fromEvent(event))
                true

            case ScheduleRequest =>
                val request = getRequestAndScheduleBehavior(event)
                openRequests += request
                Ticker(MaxRequestTicks, () => {
                    if (openRequests.contains(request)) {
                        requestLog.failed(request)
                        openRequests -= request
                    }
                    false
                })
                requestLog.add(request)

                sendNow(disposer.getId, Distributor.UserRequest, request)
                true

            case _ => false
        }
    }

    private def processRequest(partner: Int, request: Request) = request.requestType match {
        case RequestType.Get =>
            val onFinish = (success: Boolean) => if (success) requestLog.completed(request) else requestLog.failed(request)
            val process = processing.download _
            transfers.download(request.transferId, request.storageObject.size, partner, process, onFinish)
        case _ =>
            throw new IllegalStateException
    }

    private def getRequestAndScheduleBehavior(event: SimEvent): Request = {
        val behav = getBehavior(event)
        send(getId, behav.timeToNextEvent().max(0.001), ScheduleRequest, behav)
        behav.nextRequest
    }

    private def getBehavior(event: SimEvent): UserBehavior =
        event.getData match {
            case b: UserBehavior => b
            case _ => throw new IllegalStateException
        }
}

private[user] class RequestLog(log: String => Unit) {
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