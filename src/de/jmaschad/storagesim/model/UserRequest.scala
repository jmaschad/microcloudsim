package de.jmaschad.storagesim.model

import de.jmaschad.storagesim.model.storage.StorageObject

abstract class UserRequest(val user: User, time: Double)
abstract class UserObjectRequest(val storageObject: StorageObject, user: User, time: Double) extends UserRequest(user, time) {
  override def toString = "ObjectRequest from %s for %s".format(user, storageObject)
}
case class GetObject(override val storageObject: StorageObject, override val user: User, time: Double) extends UserObjectRequest(storageObject, user, time)
case class PutObject(override val storageObject: StorageObject, override val user: User, time: Double) extends UserObjectRequest(storageObject, user, time)