package de.jmaschad.storagesim.model.storage

import scala.collection.mutable

class StoreTransaction(storageObject: StorageObject, device: StorageDevice, storageSystem: StorageSystem) {
    assert(storageSystem.runningTransactions.isDefinedAt(storageObject) == false, storageObject + " has a runnning transaction")
    storageSystem.runningTransactions += (storageObject -> this)

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

    private def finish() = {
        device.removeAccessor()

        assert(storageSystem.runningTransactions.isDefinedAt(storageObject))
        storageSystem.runningTransactions -= storageObject
    }
}

class LoadTransaction(storageObject: StorageObject, device: StorageDevice, storageSystem: StorageSystem) {
    device.addAccessor()

    def complete() = device.removeAccessor
}

class StorageSystem(storageDevices: Seq[StorageDevice], initialObjects: Iterable[StorageObject]) {
    private val deviceMap = mutable.Map.empty[StorageObject, StorageDevice]
    private[storage] val runningTransactions = mutable.Map.empty[StorageObject, StoreTransaction]
    private var lastDeviceIdx = 0

    private val bucketObjectMapping = mutable.Map.empty[String, Seq[StorageObject]]
    initialObjects.foreach(obj =>
        deviceForObject(obj) match {
            case Some(dev) =>
                deviceMap += (obj -> dev)
                store(obj, dev)
            case None => throw new IllegalStateException
        })

    def buckets: Set[String] = bucketObjectMapping.keySet.toSet
    def bucket(name: String): Seq[StorageObject] = bucketObjectMapping.getOrElse(name, Seq.empty[StorageObject])

    def loadThroughput(storageObject: StorageObject): Double = deviceMap(storageObject).loadThroughput
    def storeThroughput(storageObject: StorageObject): Double = deviceMap(storageObject).storeThroughput

    def contains(storageObject: StorageObject): Boolean =
        deviceMap.isDefinedAt(storageObject) && (!runningTransactions.isDefinedAt(storageObject))
    def loadTransaction(storeageObject: StorageObject): Option[LoadTransaction] = contains(storeageObject) match {
        case true => Some(new LoadTransaction(storeageObject, deviceMap(storeageObject), this))
        case false => None
    }

    def storeTransaction(storageObject: StorageObject): Option[StoreTransaction] = deviceForObject(storageObject) match {
        case Some(device) =>
            deviceMap += (storageObject -> device)
            Some(new StoreTransaction(storageObject, device, this))
        case _ =>
            None
    }

    private[storage] def store(obj: StorageObject, dev: StorageDevice) = {
        bucketObjectMapping += obj.bucket -> (bucketObjectMapping.getOrElse(obj.bucket, Seq.empty[StorageObject]) :+ obj)
    }

    private def deviceForObject(storageObject: StorageObject): Option[StorageDevice] = {
        storageDevices.find(_.hasAvailableSpace(storageObject.size)).orElse(None)
    }
}