package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.StorageObject

abstract sealed class StatusMessage

/**
 * Send as periodic update
 */
case class CloudStatus(storageObjects: Set[StorageObject])

/**
 * Send after a MicroCloud added an object
 */
case class AddedObject(storageObject: StorageObject)

/**
 * Send when a MicroCloud finished (successful or not)
 * processing of a request.
 */
case class RequestProcessed(request: InterCloudRequest)