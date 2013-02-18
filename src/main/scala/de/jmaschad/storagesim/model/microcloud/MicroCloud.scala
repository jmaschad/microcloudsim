package de.jmaschad.storagesim.model.microcloud

import org.apache.commons.math3.distribution.UniformRealDistribution
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.predicates.PredicateType
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.ProcessingEntity
import de.jmaschad.storagesim.model.ResourceCharacteristics
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.distributor.DistributorRequest
import de.jmaschad.storagesim.model.distributor.Load
import de.jmaschad.storagesim.model.distributor.Remove
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.Downloader
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.Upload
import de.jmaschad.storagesim.model.processing.DiskIO
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.Transfer

object MicroCloud {
    private val Base = 10200

    val Initialize = Base + 1
    val Boot = Initialize + 1
    val Shutdown = Boot + 1
    val Kill = Shutdown + 1
    val MicroCloudStatus = Kill + 1
    val Request = MicroCloudStatus + 1
    val DistributorRequest = Request + 1
}

class MicroCloud(
    name: String,
    resourceCharacteristics: ResourceCharacteristics,
    failureBehavior: MicroCloudFailureBehavior,
    distributor: Distributor) extends ProcessingEntity(name, resourceCharacteristics) {

    private var state: MicroCloudState = new OnlineState
    private var firstKill = true

    def initialize(objects: Set[StorageObject]) = storageSystem.addAll(objects)

    override def log(msg: String) = Log.line("MicroCloud '%s'".format(getName), msg: String)

    override def startEntity(): Unit = {
        // start sending status messages
        sendNow(getId, MicroCloud.MicroCloudStatus)

        // schedule the next catastrophe
        scheduleKill()
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

    private def scheduleKill() = if (firstKill) {
        // additionally delay the first kill for a fraction of the MTTF. Otherwise MicroClouds
        // might initially die very soon after another. Also, don't crash during the system boot
        firstKill = false
        val meanTimeToFailure = failureBehavior.cloudFailureDistribution.getNumericalMean()
        val firstKillAdditionalDelay = new UniformRealDistribution(0.0, meanTimeToFailure).sample
        send(getId, failureBehavior.timeToCloudFailure + firstKillAdditionalDelay, MicroCloud.Kill)
    } else {
        send(getId, failureBehavior.timeToCloudFailure, MicroCloud.Kill)
    }

    private trait MicroCloudState {
        def process(event: SimEvent): Boolean

        protected def stateLog(message: String): Unit = log("[%s] %s".format(getClass().getSimpleName(), message))
    }

    private class OfflineState extends MicroCloudState {
        def process(event: SimEvent): Boolean = event.getTag match {
            case MicroCloud.Boot =>
                stateLog("received boot request")
                sendNow(distributor.getId(), Distributor.MicroCloudOnline, CloudStatus(storageSystem.objects))
                sendNow(getId, MicroCloud.MicroCloudStatus)
                scheduleKill()
                switchState(new OnlineState)
                true

            case _ => false
        }

    }

    private class OnlineState extends MicroCloudState {
        private var dueReplicationAcks = scala.collection.mutable.Map.empty[String, Seq[StorageObject]]

        def process(event: SimEvent): Boolean = event.getTag() match {
            case MicroCloud.MicroCloudStatus =>
                sendNow(distributor.getId(), Distributor.MicroCloudStatusMessage, CloudStatus(storageSystem.objects))
                send(getId(), Distributor.StatusInterval, MicroCloud.MicroCloudStatus)
                true

            case MicroCloud.Shutdown =>
                stateLog("received shutdown request")
                sendNow(distributor.getId(), Distributor.MicroCloudOffline)
                switchState(new OfflineState)
                true

            case MicroCloud.Kill =>
                stateLog("received kill request")
                resetModel
                sendNow(distributor.getId, Distributor.MicroCloudOffline)
                send(getId, failureBehavior.timeToCloudRepair, MicroCloud.Boot)
                switchState(new OfflineState)
                true

            case MicroCloud.DistributorRequest =>
                DistributorRequest.fromEvent(event) match {
                    case Load(source, obj) =>
                        val transferId = Transfer.transferId()
                        sendNow(source, MicroCloud.Request, Get(transferId, obj))

                    case Remove(obj) =>
                        storageSystem.remove(obj)
                }
                true

            case MicroCloud.Request =>
                handleCloudRequest(CloudRequest.fromEvent(event), event.getSource())
                true

            case _ => false
        }

        private def handleCloudRequest(request: CloudRequest, source: Int) = request match {
            case Get(transferId: String, obj: StorageObject) =>
                val transaction = storageSystem.loadTransaction(obj)
                uploader.start(
                    transferId,
                    obj.size,
                    source,
                    processing.loadAndUpload(_, transaction, _),
                    if (_) transaction.complete() else transaction.abort())

            case Delete(obj: StorageObject) =>
                storageSystem.remove(obj)
        }
    }
}

