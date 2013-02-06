package de.jmaschad.storagesim.model.microcloud
import de.jmaschad.storagesim.model.processing.StorageObject

abstract sealed class InterCloudRequest
case class ReplicateTo(source: Int, targets: Set[Int], bucket: String)
case class MoveTo(source: Int, target: Int, bucket: String)
case class Store(target: Int, storageObjects: Seq[StorageObject])
case class Ack(storeRequest: Store)
case class Rst(storeRequest: Store)