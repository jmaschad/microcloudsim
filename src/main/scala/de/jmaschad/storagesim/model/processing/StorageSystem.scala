package de.jmaschad.storagesim.model.processing

import scala.collection.mutable
import de.jmaschad.storagesim.model.distributor.RandomBucketBasedSelector

trait StorageTransaction {
    def complete(): Unit
    def abort(): Unit
    def throughput(): Double
}

class StoreTransaction(
    val storageObject: StorageObject,
    device: StorageDevice,
    storageSystem: StorageSystem) extends StorageTransaction {
    storageSystem.activeStore += (storageObject -> this)

    device.allocate(storageObject.size)
    device.addAccessor()

    def complete() = {
        storageSystem.store(storageObject, device)
        finish()
    }

    def abort() = {
        device.deallocate(storageObject.size)
        finish()
    }

    def throughput: Double = {
        storageSystem.storeThroughput(storageObject);
    }

    private def finish() = {
        device.removeAccessor()
        storageSystem.activeStore -= storageObject
    }
}

class LoadTransaction(val storageObject: StorageObject, device: StorageDevice, storageSystem: StorageSystem) extends StorageTransaction {
    device.addAccessor()

    def throughput: Double = {
        storageSystem.loadThroughput(storageObject);
    }

    def complete() = device.removeAccessor

    def abort() = device.removeAccessor
}

class StorageSystem(
    log: String => Unit,
    private var storageDevices: Seq[StorageDevice]) {
    private var deviceMap = Map.empty[StorageObject, StorageDevice]
    private var lastDeviceIdx = 0
    private[processing] var activeStore = Map.empty[StorageObject, StoreTransaction]

    private val bucketObjectMapping = mutable.Map.empty[String, Seq[StorageObject]]

    def add(storageObject: StorageObject) =
        deviceForObject(storageObject) match {
            case Some(dev) =>
                deviceMap += (storageObject -> dev)
                store(storageObject, dev)
            case None => throw new IllegalStateException
        }

    def reset(): StorageSystem = new StorageSystem(log, storageDevices.map(_.reset()))

    def objects: Set[StorageObject] = bucketObjectMapping.values.flatten.toSet.filter(contains(_))

    def loadThroughput(storageObject: StorageObject): Double = deviceMap(storageObject).loadThroughput
    def storeThroughput(storageObject: StorageObject): Double = deviceMap(storageObject).storeThroughput

    def contains(storageObject: StorageObject): Boolean =
        deviceMap.isDefinedAt(storageObject) && (!activeStore.isDefinedAt(storageObject))

    def loadTransaction(storeageObject: StorageObject): Option[LoadTransaction] = contains(storeageObject) match {
        case true => Some(new LoadTransaction(storeageObject, deviceMap(storeageObject), this))
        case false => None
    }

    def storeTransaction(storageObject: StorageObject): Option[StoreTransaction] =
        if (!deviceMap.isDefinedAt(storageObject)) {
            deviceForObject(storageObject) match {
                case Some(device) =>
                    deviceMap += (storageObject -> device)
                    Some(new StoreTransaction(storageObject, device, this))
                case _ =>
                    None
            }
        } else {
            None
        }

    private[processing] def store(obj: StorageObject, dev: StorageDevice) = {
        bucketObjectMapping += obj.bucket -> (bucketObjectMapping.getOrElse(obj.bucket, Seq.empty[StorageObject]) :+ obj)
    }

    private def deviceForObject(storageObject: StorageObject): Option[StorageDevice] = {
        storageDevices.find(_.hasAvailableSpace(storageObject.size)).orElse(None)
    }
}