package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.ReplicateTo
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.user.FailedRequest
import de.jmaschad.storagesim.model.user.RequestState
import Distributor._
import de.jmaschad.storagesim.model.microcloud.ReplicationDescriptor

object Distributor {
    val StatusInterval = 1
    val CheckStatusInterval = 2 * StatusInterval

    val Base = 10100
    val MicroCloudStatus = Base + 1
    val MicroCloudShutdown = MicroCloudStatus + 1
    val MicroCloudTimeout = MicroCloudShutdown + 1
    val Hartbeat = MicroCloudTimeout + 1
    val UserRequest = Hartbeat + 1
    val ReplicationFinished = UserRequest + 1
    val ReplicationTargetFailed = ReplicationFinished + 1
    val ReplicationSourceFailed = ReplicationTargetFailed + 1
}

class Distributor(name: String, distributor: RequestDistributor) extends SimEntity(name) {
    private val log = Log.line("Disposer '%s'".format(getName), _: String)

    private var onlineClouds = Map.empty[Int, Status]
    private val replicationTracker = new ReplicationTracker(log)

    override def startEntity(): Unit = {
        // wait for the first status updates
        send(getId(), 0.001, Hartbeat)
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent): Unit = event.getTag() match {
        case MicroCloudStatus =>
            val status = Status.fromEvent(event)
            log("status update from %s: %s".format(sourceEntity(event), status))
            onlineClouds += event.getSource -> status

        case MicroCloudTimeout =>
            log(sourceEntity(event) + " timed out")
            removeCloud(event.getSource())

        case MicroCloudShutdown =>
            log(sourceEntity(event) + " announced shutdown")
            removeCloud(event.getSource)

        case Hartbeat =>
            distributor.statusUpdate(onlineClouds)

            val requests = replicationTracker.trackedReplicationRequests(distributor.replicationRequests)
            requests.foreach(req => {
                sendNow(req.source, MicroCloud.InterCloudRequest, req)
            })

            send(getId(), CheckStatusInterval, Hartbeat)

        case ReplicationFinished =>
            // TODO
            val descriptor = ReplicationDescriptor.fromEvent(event)
            log("replication of %s to %s finished.".format(descriptor.bucket, CloudSim.getEntityName(descriptor.target)))

        case ReplicationTargetFailed =>
            // TODO
            val descriptor = ReplicationDescriptor.fromEvent(event)
            log("replication of %s to %s failed.".format(descriptor.bucket, CloudSim.getEntityName(descriptor.target)))

        case ReplicationSourceFailed =>
        // TODO

        case UserRequest =>
            val request = Request.fromEvent(event)

            distributor.selectMicroCloud(request) match {
                case None =>
                    log("no cloud found for %s".format(request))
                    sendNow(event.getSource(), User.RequestFailed, new FailedRequest(request, RequestState.NotFound))
                case Some(cloud) =>
                    log("foreward %s to %s".format(request, CloudSim.getEntity(cloud)))
                    sendNow(cloud, MicroCloud.UserRequest, request)
            }

        case _ => log("[online] dropped event " + event)
    }

    private def removeCloud(cloud: Int) = {
        assert(onlineClouds.contains(cloud))
        onlineClouds -= cloud
        replicationTracker.cloudsWentOflline(Seq(cloud))
    }

    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())
}
