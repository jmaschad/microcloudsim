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
case class ReplicateTo(source: Int, targets: Set[Int], bucket: String) extends InterCloudRequest
case class StoreReplica(bucket: String, transfers: Map[StorageObject, String]) extends InterCloudRequest
case class StoreAccepted(storeRequest: StoreReplica) extends InterCloudRequest
case class StoreDenied(storeRequest: StoreReplica) extends InterCloudRequest
case class SourceTimeout extends InterCloudRequest
case class SinkTimeout extends InterCloudRequest

object ReplicationDescriptor {
    def fromEvent(event: SimEvent) = event.getData() match {
        case desc: ReplicationDescriptor => desc
        case _ => throw new IllegalStateException
    }
}

class ReplicationDescriptor(val bucket: String, val target: Int)

object InterCloudHandler {
    private val StoreTimeout = 2
    private val TransactionTimeout = 2
}

private[microcloud] class InterCloudHandler(
    log: String => Unit,
    sendNow: (Int, Int, Object) => Unit,
    distributor: Distributor,
    storageSystem: StorageSystem,
    uploader: Uploader,
    downloader: Downloader,
    processing: ProcessingModel) {

    // the key is the transfer id
    var pendingLoadTransactions = Map.empty[String, LoadTransaction]

    def processRequest(eventSource: Int, request: AnyRef) = request match {
        case ReplicateTo(source, targets, bucket) =>
            log("received request to send replicate of " +
                bucket + " to " + targets.map(CloudSim.getEntityName(_)).mkString(","))

            val transfers = storageSystem.bucket(bucket).map(file => file -> Transfer.transferId).toMap
            val transactions = transfers.keys.map(so => so -> storageSystem.loadTransaction(so)).
                collect({ case (so, Some(trans)) => so -> trans }).toMap
            if (transactions.size == transfers.size) {
                val replicationRequests = targets.map(t => t -> StoreReplica(bucket, transfers))
                replicationRequests.foreach(req =>
                    sendNow(req._1, MicroCloud.InterCloudRequest, req._2))

                // start uploads
                transfers.foreach(transfer => {
                    val storageObject = transfer._1
                    val id = transfer._2
                    val transaction = transactions(storageObject)
                })
            } else {
                sendNow(distributor.getId, Distributor.ReplicationSourceFailed, bucket)
            }

        case StoreAccepted(store) =>
            log(CloudSim.getEntity(eventSource) + " accepted store replica for " + store.bucket)
            pendingLoadTransactions --= store.transfers.values

        case StoreDenied(store) =>
            log(CloudSim.getEntityName(eventSource) + " denied store replica for " + store.bucket)
            val ids = store.transfers.values
            ids.foreach(id => pendingLoadTransactions(id).abort)
            pendingLoadTransactions --= ids

            sendNow(distributor.getId, Distributor.ReplicationTargetFailed, new ReplicationDescriptor(store.bucket, eventSource))

        case request @ StoreReplica(bucket, transfers) =>
            log("received request to store replica for " + bucket)
            val transactions = transfers.keys.map(so => so -> storageSystem.storeTransaction(so)).
                collect({ case (so, Some(trans)) => so -> trans }).toMap

            if (transactions.size == transfers.size) {
                sendNow(eventSource, MicroCloud.InterCloudRequest, StoreAccepted(request))
                // start downloads
                transfers.foreach(transfer => {
                    val storageObject = transfer._1
                    val id = transfer._2
                    val transaction = transactions(storageObject)

                    downloader.start(id, storageObject.size, eventSource,
                        processing.downloadAndStore(_, transaction, _),
                        success => if (success) transaction.complete else transaction.abort)
                })
            } else {
                transactions.values.foreach(_.abort)
                sendNow(eventSource, MicroCloud.InterCloudRequest, StoreDenied(request))
            }

        case _ =>
            throw new IllegalArgumentException
    }
}