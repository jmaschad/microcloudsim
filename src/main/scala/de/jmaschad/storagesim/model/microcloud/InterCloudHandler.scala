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
import de.jmaschad.storagesim.model.distributor.ReplicationDescriptor

abstract sealed class InterCloudRequest
case class ReplicateTo(source: Int, targets: Set[Int], bucket: String) extends InterCloudRequest
case class StoreReplica(bucket: String, transfers: Map[StorageObject, String]) extends InterCloudRequest
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
        case ReplicateTo(source, targets, bucket) =>
            log("received request to send replicate of " +
                bucket + " to " + targets.map(CloudSim.getEntityName(_)).mkString(","))

            val objects = storageSystem.bucket(bucket)
            val transactions = targets.map(
                _ -> objects.map(o => o -> storageSystem.loadTransaction(o)).
                    collect({ case (so, Some(trans)) => so -> trans }).toMap).toMap

            // all ore none
            val transactionsCreated = transactions.foldLeft(Int.MaxValue)((i, trans) => i.min(trans._2.size)) == objects.size
            if (transactionsCreated) {
                transactions.foreach(trans => {
                    val target = trans._1
                    val targetTransactions = trans._2
                    val transfers = objects.map(obj => obj -> Transfer.transferId(obj)).toMap
                    sendNow(target, MicroCloud.InterCloudRequest, StoreReplica(bucket, transfers))

                    // start uploads
                    transfers.foreach(transfer => {
                        val storageObject = transfer._1
                        val id = transfer._2
                        val transaction = targetTransactions(storageObject)
                        uploader.start(id, storageObject.size, target,
                            processing.loadAndUpload(_, transaction, _),
                            success => if (success) transaction.complete else transaction.abort)
                    })
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

            sendNow(distributor.getId, Distributor.ReplicationTargetFailed, new ReplicationDescriptor(store.bucket, microCloud.getId, eventSource))

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