package de.jmaschad.storagesim.model.storage

import de.jmaschad.storagesim.model.User

class StorageObject(val name: String, val bucket: String, val size: Double, val user: User) {
  override def toString = "StorageObject '%s/%s'".format(bucket, name)
}