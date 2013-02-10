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
import de.jmaschad.storagesim.model.processing.Download
import RequestState._

private[user] class RequestLog(log: String => Unit) {
    private class LogEntry(val state: RequestState, val start: Double, val end: Double, val averageBandwidth: Double)

    private var openRequests = Set.empty[Request]
    private var requestLog = Set.empty[LogEntry]

    def add(request: Request) =
        openRequests += request

    def finish(request: Request, summary: RequestState) = {
        assert(openRequests.contains(request))
        openRequests -= request

        val start = request.time
        val end = CloudSim.clock()
        val duration = end - start
        val avgBw = if (duration <= 0) 0.0 else request.storageObject.size / duration

        requestLog += new LogEntry(summary, start, end, avgBw)
    }

    def summary(): String = {
        val avgBw = requestLog.foldLeft(0.0)((sum, entry) => sum + entry.averageBandwidth) / requestLog.size
        val bySummary = requestLog.groupBy(_.state)

        requestLog.size + " finished / " + openRequests.size + " active requests.\n" +
            "\taverage bandwidth " + (avgBw / 1024 * 8).formatted("%.2f") + "kbit/s\n" +
            "\t" + bySummary.keys.map(key => bySummary(key).size + " " + key).mkString(", ")
    }
}

object User {
    private val MaxRequestTicks = 2

    private val Base = 10300
    val RequestFailed = Base + 1
    val RequestAccepted = RequestFailed + 1
    val ScheduleRequest = RequestAccepted + 1
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

    override def log(msg: String) = Log.line("User '%s'".format(getName), msg: String)

    override def startEntity(): Unit = {
        // wait two tenth of a second for the system to start up
        behaviors.foreach(b => send(getId, 0.2 + b.timeToNextEvent, ScheduleRequest, b))
    }

    override def shutdownEntity() = log(requestLog.summary())

    override def process(event: SimEvent): Boolean = {
        event.getTag() match {
            case RequestAccepted =>
                processRequest(event.getSource, Request.fromEvent(event))
                true

            case RequestFailed =>
                val failed = FailedRequest.fromEvent(event)
                requestLog.finish(failed.request, failed.state)
                true

            case ScheduleRequest =>
                val request = getRequestAndScheduleBehavior(event)
                requestLog.add(request)
                sendNow(disposer.getId, Distributor.UserRequest, request)
                true

            case _ => false
        }
    }

    private def processRequest(partner: Int, request: Request) = request.requestType match {
        case RequestType.Get =>
            val onFinish = (success: Boolean) => if (success) requestLog.finish(request, Complete) else requestLog.finish(request, TimeOut)
            val process = processing.download _
            downloader.start(request.transferId, request.storageObject.size, partner, process, onFinish)
        case _ =>
            throw new IllegalStateException
    }

    private def getRequestAndScheduleBehavior(event: SimEvent): Request = {
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
