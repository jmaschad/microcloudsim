package de.jmaschad.storagesim.model.disposer

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent

import Disposer._
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.model.user.Request

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
    private val replicationTracker = new ReplicationTracker

    private val onlineEvents = Set(Shutdown, MicroCloudStatus, MicroCloudShutdown, Hartbeat, UserRequest, ReplicationFinished)
    private val offlineEvents = Set(ReplicationFinished)
    private var eventFilter = onlineEvents

    private val log = Log.line("Disposer '%s'".format(getName), _: String)

    override def startEntity(): Unit = {
        // wait for the first status updates
        send(getId(), 0.001, Hartbeat)
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent): Unit = eventFilter.contains(event.getTag()) match {
        case true => eventHandler(event)
        case false => log("Dropped event " + event)
    }

    private def eventHandler(event: SimEvent): Unit = event.getTag() match {
        case Shutdown =>
            log("shutdown now")
            statusTracker.onlineClouds.keys.foreach(sendNow(_, MicroCloud.Shutdown))
            eventFilter = offlineEvents

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
            updateCloudAvailabiility()
            distributor.statusUpdate(statusTracker.onlineClouds)

            val requests = replicationTracker.trackedReplicationRequests(distributor.replicationRequests)
            requests.foreach(req => sendNow(req._1, MicroCloud.SendReplica, req._2))

            send(getId(), CheckStatusInterval, Hartbeat)

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
            log("replication of bucket " + request.bucket + " to " + event.getSource() + " finished")
            replicationTracker.replicationRequestCompleted(request, event.getSource())

        case _ => log("dropped event " + event)
    }

    private def updateCloudAvailabiility() = {
        val goneOffline = statusTracker.check()
        if (goneOffline.nonEmpty) {
            replicationTracker.cloudsWentOflline(goneOffline)
            log("detected offline cloud " + goneOffline.map(CloudSim.getEntityName(_)).mkString(","))
        }
    }

    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())
}
