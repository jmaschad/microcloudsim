package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.processing.Downloader
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StoreTransaction
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StoreTransaction
import de.jmaschad.storagesim.model.processing.Uploader
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.processing.Transfer
import InterCloudHandler._
import de.jmaschad.storagesim.model.processing.LoadTransaction

abstract sealed class InterCloudRequest

object Replicate {
    def fromEvent(event: SimEvent) = event.getData() match {
        case desc: Replicate => desc
        case _ => throw new IllegalStateException
    }
}

case class Replicate(source: Int, target: Int, storageObject: StorageObject) extends InterCloudRequest {
    override def equals(obj: Any) = obj match {
        case desc: Replicate => storageObject == desc.storageObject && source == desc.source && target == desc.target
        case _ => false
    }

    override def hashCode() = 41 * (41 * (41 * (storageObject.hashCode() + 41) + source) + target)

    override def toString = "Replicate " + storageObject + " from " + CloudSim.getEntityName(source) + " to " + CloudSim.getEntityName(target)
}

case class StoreReplica(storageObject: StorageObject, transferId: String) extends InterCloudRequest
case class StoreAccepted(storeRequest: StoreReplica) extends InterCloudRequest
case class StoreDenied(storeRequest: StoreReplica) extends InterCloudRequest
case class SourceTimeout extends InterCloudRequest
case class SinkTimeout extends InterCloudRequest

object InterCloudHandler {
    private val StoreTimeout = 2
    private val TransactionTimeout = 2
}

private[microcloud] class InterCloudHandler(
    log: String => Unit,
    sendNow: (Int, Int, Object) => Unit,
    microCloud: MicroCloud,
    distributor: Distributor,
    storageSystem: StorageSystem,
    uploader: Uploader,
    downloader: Downloader,
    processing: ProcessingModel) {

    // the key is the transfer id
    var pendingLoadTransactions = Map.empty[String, LoadTransaction]

    def processRequest(eventSource: Int, request: AnyRef) = request match {
        case req @ Replicate(source, target, obj) =>
            log("received request to send replicate of " +
                obj + " to " + CloudSim.getEntityName(target))

            storageSystem.loadTransaction(obj) match {
                case Some(transaction) =>
                    val transferId = Transfer.transferId(obj)
                    sendNow(target, MicroCloud.InterCloudRequest, StoreReplica(obj, transferId))
                    uploader.start(transferId, obj.size, target,
                        processing.loadAndUpload(_, transaction, _),
                        success => if (success) {
                            transaction.complete
                        } else {
                            transaction.abort
                            sendNow(distributor.getId(), Distributor.ReplicationTargetFailed, target: java.lang.Integer)
                        })
                    pendingLoadTransactions += transferId -> transaction
                case None =>
                    throw new IllegalStateException
            }

        case StoreAccepted(store) =>
            log(CloudSim.getEntity(eventSource) + " accepted store replica for " + store.storageObject)
            pendingLoadTransactions -= store.transferId

        case req @ StoreDenied(store) =>
            log(CloudSim.getEntityName(eventSource) + " denied store replica for " + store.storageObject)
            pendingLoadTransactions(store.transferId).abort
            pendingLoadTransactions -= store.transferId
            sendNow(distributor.getId(), Distributor.ReplicationTargetFailed, eventSource: java.lang.Integer)

        case request @ StoreReplica(obj, transferId) =>
            log("received request to store replica for " + obj)

            storageSystem.storeTransaction(obj) match {
                case Some(transaction) =>
                    sendNow(eventSource, MicroCloud.InterCloudRequest, StoreAccepted(request))
                    // start download
                    downloader.start(transferId, obj.size, eventSource,
                        processing.downloadAndStore(_, transaction, _),
                        success => if (success) {
                            transaction.complete
                        } else {
                            transaction.abort
                            sendNow(distributor.getId(), Distributor.ReplicationSourceFailed, eventSource: java.lang.Integer)
                        })

                case None =>
                    sendNow(eventSource, MicroCloud.InterCloudRequest, StoreDenied(request))
            }

        case _ =>
            throw new IllegalArgumentException
    }
}