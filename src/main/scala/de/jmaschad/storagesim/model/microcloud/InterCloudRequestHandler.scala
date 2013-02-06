package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.TransferModel
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageSystem
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import InterCloudRequestHandler._
import de.jmaschad.storagesim.util.Ticker
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StoreTransaction
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StoreTransaction

object InterCloudRequestHandler {
    private val StoreTimeout = 2
    private val TransactionTimeout = 2
}

private[microcloud] class InterCloudRequestHandler(
    log: String => Unit,
    sendNow: (Int, Int, Object) => Unit,
    storageSystem: StorageSystem,
    transfers: TransferModel,
    processing: ProcessingModel) {

    var openStore = Set.empty[Store]
    var waitingStoreTransactions = Map.empty[StorageObject, StoreTransaction]
    var activeStoreTransactions = Map.empty[StorageObject, StoreTransaction]

    def processRequest(request: AnyRef) = request match {
        case ReplicateTo(source, targets, bucket) =>
            log("received request to send replicate of " +
                bucket + " to " + targets.map(CloudSim.getEntityName(_)).mkString(","))
            val objects = storageSystem.bucket(bucket)

            targets.foreach(target => {
                val store = Store(target, objects)
                openStore += store
                Ticker(StoreTimeout, {
                    openStore -= store
                    false
                })

                sendNow(target, MicroCloud.InterCloudRequest, store)
            })

        case Ack(store) =>
            assert(openStore.contains(store))
            openStore -= store
            store.storageObjects.foreach(obj => {
                upload(store.target, obj)
            })

        case Rst(store) =>
            assert(openStore.contains(store))
            openStore -= store
            log(store + " rejected")

        case Store(transferID, storageObjects) =>
            log("received request to store replica for " + storageObjects.mkString(","))

        case _ =>
            throw new IllegalArgumentException
    }

    def upload(target: Int, obj: StorageObject) = {}
}