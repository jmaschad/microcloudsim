package de.jmaschad.storagesim.model.storage

import scala.collection.mutable

class StorageSystem(storageDevices: Seq[StorageDevice]) {
  private val deviceMap = mutable.Map.empty[StorageObject, StorageDevice]
  private val currentUpload = mutable.Map.empty[StorageObject, StorageDevice]
  private var lastDeviceIdx = 0

  def storeObject(storageObject: StorageObject): Unit = {
    store(storageObject, deviceForObject(storageObject).getOrElse(throw new IllegalStateException))
  }

  private def deviceForObject(storageObject: StorageObject): Option[StorageDevice] = {
    storageDevices.find(_.hasAvailableSpace(storageObject.size)).orElse(None)
  }

  private def store(storageObject: StorageObject, storageDevice: StorageDevice): Unit = {
    deviceMap += (storageObject -> storageDevice)
    storageDevice.allocate(storageObject.size)
  }

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

    deviceMap += (storeageObject -> device)
    currentUpload -= storeageObject
    device.removeAccessor
  }

  def abortStore(storeageObject: StorageObject) = {
    val device = currentUpload(storeageObject)

    currentUpload -= storeageObject
    device.deallocate(storeageObject.size)
    device.removeAccessor()
  }
}