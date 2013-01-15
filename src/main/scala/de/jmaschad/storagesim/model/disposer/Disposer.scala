package de.jmaschad.storagesim.model.disposer

import scala.collection.mutable

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent

import Disposer._
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.User
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.model.request.Request

object Disposer {
    val StatusInterval = 1
    val CheckStatusInterval = 2 * StatusInterval

    val Base = 10100
    val Shutdown = Base + 1
    val MicroCloudStatus = Shutdown + 1
    val MicroCloudShutdown = MicroCloudStatus + 1
    val Hartbeat = MicroCloudShutdown + 1
    val UserRequest = Hartbeat + 1
    val ReplicationFinished = UserRequest + 1
}

class Disposer(name: String, distributor: RequestDistributor) extends SimEntity(name) {
    private val statusTracker = new StatusTracker
    private val runningReplicationRequests = mutable.Map.empty[ReplicationRequest, Set[Int]]
    private var eventHandler = onlineEventHandler _

    private val log = Log.line("Disposer '%s'".format(getName), _: String)

    override def startEntity(): Unit = {
        // wait one tenth of a second for the first status updates
        send(getId(), 0.001, Hartbeat)
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent): Unit = eventHandler(event)

    private def onlineEventHandler(event: SimEvent): Unit = event.getTag() match {
        case Shutdown =>
            log("shutdown now")
            statusTracker.onlineClouds.keys.foreach(sendNow(_, MicroCloud.Shutdown))
            eventHandler = offlineEventHandler _

        case MicroCloudStatus =>
            val status = (event.getData() match {
                case s: Status => s
                case _ => throw new IllegalArgumentException
            })
            log("status update from %s: %s".format(sourceEntity(event), status))
            statusTracker.online(event.getSource(), status)

        case MicroCloudShutdown =>
            log("shutdown notification from %s".format(sourceEntity(event)))
            statusTracker.offline(event.getSource)

        case Hartbeat =>
            send(getId(), CheckStatusInterval, Hartbeat)
            removeOffline()
            distributor.statusUpdate(statusTracker.onlineClouds)
            updateReplication()

        case UserRequest =>
            val request = event.getData() match {
                case r: Request => r
                case _ => throw new IllegalStateException
            }

            distributor.selectMicroCloud(request) match {
                case None =>
                    log("no cloud found for %s".format(request))
                    sendNow(event.getSource(), User.RequestFailed, request)
                case Some(cloud) =>
                    log("foreward %s to %s".format(request, CloudSim.getEntity(cloud)))
                    sendNow(cloud, MicroCloud.UserRequest, request)
            }

        case ReplicationFinished =>
            val request = event.getData() match {
                case req: ReplicationRequest => req
                case _ => throw new IllegalArgumentException
            }
            assert(runningReplicationRequests.contains(request))
            assert(runningReplicationRequests(request).contains(event.getSource()))

            log("replication of bucket " + request.bucket + " to " + event.getSource() + " finished")

            runningReplicationRequests(request) = runningReplicationRequests(request) - event.getSource()
            if (runningReplicationRequests(request).isEmpty) {
                runningReplicationRequests -= request
            }

        case _ => log("dropped event " + event)
    }

    private def offlineEventHandler(event: SimEvent) = log("Offline! dropped event " + event)

    private def removeOffline() = {
        val goneOffline = statusTracker.check()
        if (goneOffline.nonEmpty) {
            runningReplicationRequests.foreach(reqCloudsMapping =>
                runningReplicationRequests(reqCloudsMapping._1) = runningReplicationRequests(reqCloudsMapping._1) -- goneOffline)
            log("detected offline cloud " + goneOffline.mkString(","))
        }
    }

    private def updateReplication() = {
        val newRequests = distributor.replicationRequests.filter(req => !runningReplicationRequests.contains(req._2))
        newRequests.foreach(cloudReq =>
            sendNow(cloudReq._1, MicroCloud.SendReplica, cloudReq._2))
        runningReplicationRequests ++= newRequests.map(req => req._2 -> req._2.targets.toSet)
    }

    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())

}