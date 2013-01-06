package de.jmaschad.storagesim.model

import de.jmaschad.storagesim.model.storage.StorageObject

abstract class UserRequest(val user: User)
abstract class UserObjectRequest(val storageObject: StorageObject, user: User) extends UserRequest(user) {
  override def toString = "ObjectRequest from %s for %s".format(user, storageObject)
}
case class GetObject(override val storageObject: StorageObject, override val user: User) extends UserObjectRequest(storageObject, user)
case class PutObject(override val storageObject: StorageObject, override val user: User) extends UserObjectRequest(storageObject, user)