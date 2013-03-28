package de.jmaschad.storagesim.model.processing

import scala.collection.mutable
import de.jmaschad.storagesim.model.distributor.RandomBucketBasedSelector

class StorageSystem(
    log: String => Unit,
    private var storageDevices: Seq[StorageDevice]) {
    private var storedObjects = Set.empty[StorageObject]
    private var availableCapacity = { storageDevices map { _.capacity } sum }

    def objects: Set[StorageObject] = storedObjects

    def addAll(objs: Set[StorageObject]) = objs foreach { add(_) }

    def add(obj: StorageObject) =
        if (availableCapacity > obj.size) {
            availableCapacity -= obj.size
            storedObjects += obj
        } else {
            throw new IllegalStateException
        }

    def remove(obj: StorageObject) =
        if (storedObjects.contains(obj)) {
            storedObjects -= obj
            availableCapacity += obj.size
        } else {
            throw new IllegalStateException
        }

    def reset(): StorageSystem = new StorageSystem(log, storageDevices)

    def isEmpty = storedObjects.isEmpty

    def contains(storageObject: StorageObject): Boolean = storedObjects.contains(storageObject)
}