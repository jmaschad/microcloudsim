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

case class ReplicationSourceFailed(source: Int)
case class ReplicationTargetFailed(target: Int)
case class ReplicationFinished(storageObject: StorageObject)