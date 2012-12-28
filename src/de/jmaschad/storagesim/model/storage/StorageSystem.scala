package de.jmaschad.storagesim.model.storage

class StorageSystem(storageDevices: Seq[StorageDevice]) {
  private val deviceMap = Map[StorageObject, StorageDevice]()

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