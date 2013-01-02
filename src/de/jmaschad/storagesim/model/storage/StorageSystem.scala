package de.jmaschad.storagesim.model.storage

import scala.collection.mutable

class StorageSystem(storageDevices: Seq[StorageDevice]) {
  private val deviceMap = mutable.Map.empty[StorageObject, StorageDevice]
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

  def loadThroughput(storageObject: StorageObject): Double = 0.0
  def storeThroughput(storageObject: StorageObject): Double = 0.0

  def contains(storageObject: StorageObject): Boolean = deviceMap.isDefinedAt(storageObject)
  def startLoad(storeageObject: StorageObject) = {}
  def finishLoad(storeageObject: StorageObject) = {}

  def allocate(storageObject: StorageObject): Boolean = false
  def startStore(storeageObject: StorageObject) = {}
  def finishStore(storeageObject: StorageObject) = {}
  def abortStore(storeageObject: StorageObject) = {}
}