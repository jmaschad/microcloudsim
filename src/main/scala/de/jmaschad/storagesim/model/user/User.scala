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
import de.jmaschad.storagesim.model.microcloud.CloudRequest
import de.jmaschad.storagesim.model.microcloud.Get

private[user] class RequestLog(
    log: String => Unit) {
    private class Active(size: Double) {
        private val start = CloudSim.clock()
        def finish(summary: RequestState): Finished = {
            val duration = CloudSim.clock() - start
            new Finished(duration, size / duration, summary)
        }
    }

    private class Finished(val duration: Double, val avgBandwidth: Double, val summary: RequestState) {
        override def toString = "%s %.3fs %.3f MBit/s".format(summary, duration, avgBandwidth * 8)
    }

    private var activeRequests = Map.empty[CloudRequest, Active]
    private var finishedRequests = Set.empty[Finished]

    def add(request: CloudRequest) = request match {
        case Get(_, obj) =>
            assert(!activeRequests.isDefinedAt(request))
            activeRequests += request -> new Active(obj.size)

        case _ =>
            throw new IllegalStateException
    }

    def finish(request: CloudRequest, summary: RequestState) = {
        assert(activeRequests.contains(request))
        val finished = activeRequests(request).finish(summary)
        log("request finished: " + finished)
        finishedRequests += activeRequests(request).finish(summary)
        activeRequests -= request
    }

    def summary(): String = {
        val avgBw = finishedRequests.foldLeft(0.0)((sum, entry) => sum + entry.avgBandwidth) / finishedRequests.count(_.avgBandwidth > 0.0)
        val bySummary = finishedRequests.groupBy(_.summary)

        finishedRequests.size + " finished / " + activeRequests.size + " active requests.\n" +
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

    override def process(event: SimEvent): Boolean =
        event.getTag() match {
            case ScheduleRequest =>
                getRequestAndScheduleBehavior(event) match {
                    case get @ Get(_, _) => sendGet(get)
                    case _ => throw new IllegalStateException
                }
                true

            case _ => false
        }

    private def sendGet(get: Get) = {
        requestLog.add(get)
        distributor.cloudForGet(get) match {
            case Right(cloud) =>
                sendNow(cloud, MicroCloud.Request, get)
                downloader.start(
                    get.transferId,
                    get.obj.size,
                    cloud,
                    processing.download _,
                    if (_) {
                        requestLog.finish(get, Complete)
                    } else {
                        requestLog.finish(get, TimeOut)
                    })

            case Left(error) =>
                requestLog.finish(get, error)
        }
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
