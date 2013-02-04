package de.jmaschad.storagesim.model.processing

import scala.collection.mutable

trait StorageTransaction {
    def complete(): Unit
    def abort(): Unit
    def throughput(): Double
}

class StoreTransaction(val storageObject: StorageObject, device: StorageDevice, storageSystem: StorageSystem) extends StorageTransaction {
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

    def throughput: Double = {
        storageSystem.storeThroughput(storageObject);
    }

    private def finish() = {
        device.removeAccessor()

        assert(storageSystem.runningTransactions.isDefinedAt(storageObject))
        storageSystem.runningTransactions -= storageObject
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

class StorageSystem(storageDevices: Seq[StorageDevice], initialObjects: Iterable[StorageObject]) {
    private val deviceMap = mutable.Map.empty[StorageObject, StorageDevice]
    private[processing] val runningTransactions = mutable.Map.empty[StorageObject, StoreTransaction]
    private var lastDeviceIdx = 0

    private val bucketObjectMapping = mutable.Map.empty[String, Seq[StorageObject]]
    initialObjects.foreach(obj =>
        deviceForObject(obj) match {
            case Some(dev) =>
                deviceMap += (obj -> dev)
                store(obj, dev)
            case None => throw new IllegalStateException
        })

    def reset() = runningTransactions.mapValues(_.abort)

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

    private[processing] def store(obj: StorageObject, dev: StorageDevice) = {
        bucketObjectMapping += obj.bucket -> (bucketObjectMapping.getOrElse(obj.bucket, Seq.empty[StorageObject]) :+ obj)
    }

    private def deviceForObject(storageObject: StorageObject): Option[StorageDevice] = {
        storageDevices.find(_.hasAvailableSpace(storageObject.size)).orElse(None)
    }
}