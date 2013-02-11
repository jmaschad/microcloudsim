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

    val Base = 10100
    val MicroCloudStatus = Base + 1
    val MicroCloudShutdown = MicroCloudStatus + 1
    val MicroCloudTimeout = MicroCloudShutdown + 1
    val UpdateCloudModel = MicroCloudTimeout + 1
    val UserRequest = UpdateCloudModel + 1
    val ReplicationFinished = UserRequest + 1
    val ReplicationSourceFailed = ReplicationFinished + 1
    val ReplicationTargetFailed = ReplicationSourceFailed + 1
}
import Distributor._

class Distributor(name: String) extends SimEntity(name) {
    private var onlineClouds = Map.empty[Int, Status]
    private val replicationTracker = new ReplicationController(log _, sendNow _, selector)
    private val selector = new RandomCloudSelector(log _, sendNow _)

    override def startEntity(): Unit = {
        // wait for the first status updates
        send(getId(), StatusInterval * 0.5, UpdateCloudModel)
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

        case UpdateCloudModel =>
            selector.statusUpdate(onlineClouds)
            send(getId(), StatusInterval, UpdateCloudModel)

        case ReplicationFinished =>
            val request = Replicate.fromEvent(event)
            replicationTracker.requestFinished(request)
            log(request + " finished.")

        case ReplicationSourceFailed =>
            val source = event.getData() match {
                case t: java.lang.Integer => t
                case _ => throw new IllegalStateException
            }
            replicationTracker.requestSourceFailed(source)

        case ReplicationTargetFailed =>
            val target = event.getData() match {
                case t: java.lang.Integer => t
                case _ => throw new IllegalStateException
            }
            replicationTracker.requestTargetFailed(target)

        case UserRequest =>
            val request = Request.fromEvent(event)

            selector.selectMicroCloud(request) match {
                case None =>
                    log("no cloud found for %s".format(request))
                    sendNow(event.getSource(), User.RequestFailed, new FailedRequest(request, RequestState.NotFound))
                case Some(cloud) =>
                    log("foreward %s to %s".format(request, CloudSim.getEntity(cloud)))
                    sendNow(cloud, MicroCloud.UserRequest, request)
            }

        case _ => log("[online] dropped event " + event)
    }

    private def log(msg: String) = Log.line("Distributor '%s'".format(getName), msg: String)

    private def removeCloud(cloud: Int) = {
        assert(onlineClouds.contains(cloud))
        onlineClouds -= cloud
        selector.repairOfflineCloud(cloud)
    }

    private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())
}
