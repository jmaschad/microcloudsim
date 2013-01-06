package de.jmaschad.storagesim.model.storage

class StorageObject(val bucket: String, val name: String, val size: Double) {
  override def toString = "StorageObject %s/%s".format(bucket, name)
}