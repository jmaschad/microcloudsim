package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.Replicate
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.user.FailedRequest
import de.jmaschad.storagesim.model.user.RequestState

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
    val ReplicationFailed = ReplicationFinished + 1
}
import Distributor._

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

        // detected timeout of microcloud
        case MicroCloudTimeout =>
            log(sourceEntity(event) + " timed out")
            removeCloud(event.getSource())

        case MicroCloudShutdown =>
            log(sourceEntity(event) + " announced shutdown")
            removeCloud(event.getSource)

        case Hartbeat =>
            distributor.statusUpdate(onlineClouds)
            sendReplicationRequests()
            send(getId(), CheckStatusInterval, Hartbeat)

        case ReplicationFinished =>
            val request = Replicate.fromEvent(event)
            replicationTracker.requestFinished(request)
            log(request + " finished.")

        case ReplicationFailed =>
            val request = Replicate.fromEvent(event)
            replicationTracker.requestFailed(request)
            log(request + " failed.")

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
        distributor.statusUpdate(onlineClouds)
        replicationTracker.cloudWentOflline(cloud)
        sendReplicationRequests()
    }

    private def sendReplicationRequests() = {
        val requests = replicationTracker.trackedReplicationRequests(distributor.replicationRequests)
        requests.foreach(req => {
            sendNow(req.source, MicroCloud.InterCloudRequest, req)
        })
    }

    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())
}
