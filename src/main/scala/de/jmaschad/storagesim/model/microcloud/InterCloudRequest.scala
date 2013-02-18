package de.jmaschad.storagesim.model.microcloud

import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject

abstract sealed class InterCloudRequest(val requestHandler: Int)
case class Load(source: Int, target: Int, storageObject: StorageObject) extends InterCloudRequest(target)
case class CancelRequest(request: InterCloudRequest) extends InterCloudRequest(request.requestHandler)
case class Remove(target: Int, storageObject: StorageObject) extends InterCloudRequest(target)