package de.jmaschad.storagesim.model.microcloud

import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import org.cloudbus.cloudsim.core.SimEvent

abstract sealed class InterCloudRequest
case class Send(storageObject: StorageObject) extends InterCloudRequest
