package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.microcloud.Replicate
import de.jmaschad.storagesim.model.processing.StorageObject

class ReplicationTracker(var requests: Set[Replicate]) {
    val start = CloudSim.clock()
}