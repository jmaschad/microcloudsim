package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.predicates.PredicateType

import MicroCloud._
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.ProcessingEntity
import de.jmaschad.storagesim.model.ResourceCharacteristics
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.TransferModel

object MicroCloud {
    private val Base = 10200

    val Boot = Base + 1
    val Shutdown = Boot + 1
    val Kill = Shutdown + 1
    val MicroCloudStatus = Kill + 1
    val UserRequest = MicroCloudStatus + 1
    val InterCloudRequest = UserRequest + 1
}

class MicroCloud(
    name: String,
    resourceCharacteristics: ResourceCharacteristics,
    initialObjects: Iterable[StorageObject],
    failureBehavior: MicroCloudFailureBehavior,
    disposer: Distributor) extends ProcessingEntity(name, resourceCharacteristics, initialObjects) {
    private val userRequests = new UserRequestHandler(log _, sendNow _, storageSystem, transfers, processing)
    private val interCloudRequests = new InterCloudRequestHandler(log _, sendNow _, storageSystem, transfers, processing)
    private var state: MicroCloudState = new OfflineState

    def scheduleProcessingUpdate(delay: Double) = {
        CloudSim.cancelAll(getId(), new PredicateType(ProcessingModel.ProcUpdate))
        send(getId(), delay, ProcessingModel.ProcUpdate)
    }

    def status = Status(storageSystem.buckets)

    override def log(msg: String) = Log.line("MicroCloud '%s'".format(getName), msg: String)

    override def startEntity(): Unit = {
        send(getId(), 0.0, Boot)
    }

    override def shutdownEntity() = {
        log(processing.jobCount + " running jobs on shutdown")
    }

    override def process(event: SimEvent): Boolean = {
        state.process(event)
    }

    override def toString = "%s %s".format(getClass.getSimpleName, getName)

    private def switchState(newState: MicroCloudState): Unit = {
        state = newState
    }

    private trait MicroCloudState {
        def process(event: SimEvent): Boolean

        protected def stateLog(message: String): Unit = log("[%s] %s".format(getClass().getSimpleName(), message))
    }

    private class OfflineState extends MicroCloudState {
        def process(event: SimEvent): Boolean = event.getTag match {
            case MicroCloud.Boot =>
                stateLog("received boot request")
                sendNow(getId, MicroCloud.MicroCloudStatus)
                send(getId, failureBehavior.timeToCloudFailure, Kill)
                switchState(new OnlineState)
                true

            case _ => false
        }
    }

    private class OnlineState extends MicroCloudState {
        private var dueReplicationAcks = scala.collection.mutable.Map.empty[String, Seq[StorageObject]]

        def process(event: SimEvent): Boolean = event.getTag() match {
            case MicroCloudStatus =>
                sendNow(disposer.getId(), Distributor.MicroCloudStatus, status)
                send(getId(), Distributor.StatusInterval, MicroCloudStatus)
                true

            case Shutdown =>
                stateLog("received shutdown request")
                sendNow(disposer.getId(), Distributor.MicroCloudShutdown)
                switchState(new OfflineState)
                true

            case Kill =>
                stateLog("received kill request")
                resetModel
                send(getId, failureBehavior.timeToCloudRepair, Boot)
                switchState(new OfflineState)
                true

            case UserRequest =>
                userRequests.process(event)
                true

            case InterCloudRequest =>
                interCloudRequests.processRequest(event.getData())
                true

            case _ => false
        }
    }
}

