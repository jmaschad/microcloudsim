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

object Copy {
    def fromEvent(event: SimEvent) = event.getData() match {
        case desc: Copy => desc
        case _ => throw new IllegalStateException
    }
}

case class Copy(source: Int, target: Int, storageObject: StorageObject) extends InterCloudRequest
case class CancelCopy(copy: Copy)
case class Remove(storageObject: StorageObject)
case class Store(storageObject: StorageObject, transferId: String) extends InterCloudRequest
case class StoreAccepted(storeRequest: Store) extends InterCloudRequest
case class StoreDenied(storeRequest: Store) extends InterCloudRequest
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
    var activeCopies = Map.empty[Copy, String]

    def processRequest(eventSource: Int, request: AnyRef) = request match {
        case req @ Copy(source, target, obj) =>
            log("received request to send replicate of " +
                obj + " to " + CloudSim.getEntityName(target))

            storageSystem.loadTransaction(obj) match {
                case Some(transaction) =>
                    val transferId = Transfer.transferId(obj)
                    activeCopies += req -> transferId

                    sendNow(target, MicroCloud.InterCloudRequest, Store(obj, transferId))
                    uploader.start(transferId, obj.size, target,
                        processing.loadAndUpload(_, transaction, _),
                        success => {
                            assert(activeCopies.contains(req))
                            activeCopies -= req

                            if (success) {
                                transaction.complete
                            } else {
                                transaction.abort
                                sendNow(distributor.getId(), Distributor.MicroCloudStatusMessage, RequestProcessed(req))
                            }
                        })
                case None =>
                    throw new IllegalStateException
            }

        case CancelCopy(copy) =>
            uploader.cancel(activeCopies(copy))

        case StoreAccepted(store) =>
            log(CloudSim.getEntity(eventSource) + " accepted store replica for " + store.storageObject)

        case req @ StoreDenied(store) =>
            log(CloudSim.getEntityName(eventSource) + " denied store replica for " + store.storageObject)
            uploader.cancel(store.transferId)
            sendNow(distributor.getId(), Distributor.MicroCloudStatusMessage, RequestProcessed(req))

        case request @ Store(obj, transferId) =>
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
                        })

                case None =>
                    sendNow(eventSource, MicroCloud.InterCloudRequest, StoreDenied(request))
            }

        case _ =>
            throw new IllegalArgumentException
    }
}