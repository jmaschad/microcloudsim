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
import User._
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.microcloud.MicroCloud

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
        val avgBw = requestLog.foldLeft(0.0)((sum, entry) => sum + entry.averageBandwidth) / requestLog.count(_.averageBandwidth > 0.0)
        val bySummary = requestLog.groupBy(_.state)

        requestLog.size + " finished / " + openRequests.size + " active requests.\n" +
            "\taverage bandwidth " + (avgBw * 8).formatted("%.2f") + "Mbit/s\n" +
            "\t" + bySummary.keys.map(key => bySummary(key).size + " " + key).mkString(", ")
    }
}

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

    private var openRequests = Set.empty[Request]

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

    override def process(event: SimEvent): Boolean = {
        event.getTag() match {

            case ScheduleRequest =>
                val request = getRequestAndScheduleBehavior(event)
                requestLog.add(request)
                distributor.selectCloudFor(request) match {
                    case Right(cloud) =>
                        sendNow(cloud, MicroCloud.UserRequest, request)

                        val onFinish = (success: Boolean) => if (success) {
                            requestLog.finish(request, Complete)
                        } else {
                            requestLog.finish(request, TimeOut)
                        }
                        downloader.start(request.transferId, request.storageObject.size, cloud, processing.download _, onFinish)

                    case Left(error) =>
                        requestLog.finish(request, error)
                }
                true

            case _ => false
        }
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
