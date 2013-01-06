package de.jmaschad.storagesim.model.storage

import scala.collection.mutable

class StorageSystem(storageDevices: Seq[StorageDevice], initialObjects: Iterable[StorageObject]) {
  private val deviceMap = mutable.Map.empty[StorageObject, StorageDevice]
  private val currentUpload = mutable.Map.empty[StorageObject, StorageDevice]
  private var lastDeviceIdx = 0
  private val bucketSet = mutable.Set.empty[String]

  initialObjects.foreach(obj =>
    deviceForObject(obj) match {
      case Some(dev) => store(obj, dev)
      case None => throw new IllegalStateException
    })

  def buckets = bucketSet

  def loadThroughput(storageObject: StorageObject): Double = deviceMap(storageObject).loadThroughput
  def storeThroughput(storageObject: StorageObject): Double = deviceMap(storageObject).storeThroughput

  def contains(storageObject: StorageObject): Boolean = deviceMap.isDefinedAt(storageObject)
  def startLoad(storeageObject: StorageObject) = deviceMap(storeageObject).addAccessor
  def finishLoad(storeageObject: StorageObject) = deviceMap(storeageObject).removeAccessor

  def allocate(storageObject: StorageObject): Boolean = deviceForObject(storageObject) match {
    case Some(dev) =>
      dev.allocate(storageObject.size)
      currentUpload += (storageObject -> dev)
      true

    case _ => false
  }

  def startStore(storeageObject: StorageObject) = deviceMap(storeageObject).addAccessor
  def finishStore(storeageObject: StorageObject) = {
    val device = currentUpload(storeageObject)
    store(storeageObject, device)
    finishUpload(storeageObject, device)
  }

  def abortStore(storeageObject: StorageObject) = {
    val device = currentUpload(storeageObject)
    device.deallocate(storeageObject.size)

    finishUpload(storeageObject, device)
  }

  private def finishUpload(obj: StorageObject, dev: StorageDevice) = {
    currentUpload -= obj
    dev.removeAccessor()
  }

  private def deviceForObject(storageObject: StorageObject): Option[StorageDevice] = {
    storageDevices.find(_.hasAvailableSpace(storageObject.size)).orElse(None)
  }

  private def store(obj: StorageObject, dev: StorageDevice) = {
    deviceMap += (obj -> dev)
    bucketSet += obj.bucket
  }

}