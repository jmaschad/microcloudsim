package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.microcloud.Replicate
import org.cloudbus.cloudsim.core.SimEvent
import java.util.Objects
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.Replicate

class ReplicationController(selector: CloudSelector) {
    var activeReplications = Set.empty[Replicate]

    def trackedReplicationRequests(descriptors: Set[Replicate]): Set[Replicate] = {
        val newDescriptors = descriptors.diff(activeReplications)
        activeReplications ++= newDescriptors

        newDescriptors.map(d => Replicate(d.source, d.target, d.storageObject))
    }

    def requestFinished(request: Replicate) = activeReplications -= request

    def requestFailed(request: Replicate) = requestFinished(request)

    def cloudWentOflline(cloud: Int) =
        activeReplications --= activeReplications.filter(desc => desc.source == cloud || desc.target == cloud)
}

