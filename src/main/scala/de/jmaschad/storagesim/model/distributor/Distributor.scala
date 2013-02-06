package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import Distributor._
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.microcloud.ReplicateTo

object Distributor {
    val StatusInterval = 1
    val CheckStatusInterval = 2 * StatusInterval

    val Base = 10100
    val MicroCloudStatus = Base + 1
    val MicroCloudShutdown = MicroCloudStatus + 1
    val Hartbeat = MicroCloudShutdown + 1
    val UserRequest = Hartbeat + 1
    val ReplicationFinished = UserRequest + 1
}

class Distributor(name: String, distributor: RequestDistributor) extends SimEntity(name) {
    private val log = Log.line("Disposer '%s'".format(getName), _: String)

    private val statusTracker = new StatusTracker
    private val replicationTracker = new ReplicationTracker(log)

    override def startEntity(): Unit = {
        // wait for the first status updates
        send(getId(), 0.001, Hartbeat)
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent): Unit = event.getTag() match {
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
            replicationTracker.cloudsWentOflline(Seq(event.getSource))

        case ReplicationFinished =>
            val request = event.getData() match {
                case req: ReplicateTo => req
                case _ => throw new IllegalArgumentException
            }
            log("replication of %s to %s finished.".format(request.bucket, CloudSim.getEntityName(event.getSource())))
            replicationTracker.replicationRequestCompleted(request, event.getSource())

        case Hartbeat =>
            updateCloudAvailability()
            distributor.statusUpdate(statusTracker.onlineClouds)

            val requests = replicationTracker.trackedReplicationRequests(distributor.replicationRequests)
            requests.foreach(req => {
                sendNow(req.source, MicroCloud.InterCloudRequest, req)
            })

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

        case _ => log("[online] dropped event " + event)
    }

    private def updateCloudAvailability() = {
        val goneOffline = statusTracker.check()
        if (goneOffline.nonEmpty) {
            replicationTracker.cloudsWentOflline(goneOffline)
            log("detected offline cloud " + goneOffline.map(CloudSim.getEntityName(_)).mkString(","))
        }
    }

    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())
}
