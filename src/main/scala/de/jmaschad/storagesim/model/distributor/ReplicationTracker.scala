package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.microcloud.ReplicateTo
import org.cloudbus.cloudsim.core.SimEvent
import java.util.Objects

object ReplicationDescriptor {
    def fromEvent(event: SimEvent) = event.getData() match {
        case desc: ReplicationDescriptor => desc
        case _ => throw new IllegalStateException
    }
}

class ReplicationDescriptor(val bucket: String, val source: Int, val target: Int) {
    override def equals(obj: Any) = obj match {
        case desc: ReplicationDescriptor => bucket == desc.bucket && source == desc.source && target == desc.target
        case _ => false
    }

    override def hashCode() = 41 * (41 * (41 * (bucket.hashCode() + 41) + source) + target)
}

private[distributor] final class ReplicationTracker(private val log: String => Unit) {
    var activeReplications = Set.empty[ReplicationDescriptor]

    def trackedReplicationRequests(descriptors: Set[ReplicationDescriptor]): Set[ReplicateTo] = {
        val newDescriptors = descriptors.diff(activeReplications)
        activeReplications ++= newDescriptors

        val sourceBucketGroups = newDescriptors.groupBy(_.source).map(m => m._1 -> m._2.groupBy(_.bucket))
        val requests = sourceBucketGroups.flatMap(s => {
            val source = s._1
            s._2.map(b => {
                val bucket = b._1
                ReplicateTo(source, b._2.map(_.target), bucket)
            })
        })
        requests.toSet
    }

    def requestFinished(descriptor: ReplicationDescriptor) = {
        activeReplications -= descriptor
    }

    def targetFailed(descriptor: ReplicationDescriptor) = {
        requestFinished(descriptor)
    }

    def sourceFailed(source: Int) {
        activeReplications --= activeReplications.filter(_.source == source)
    }

    def cloudWentOflline(cloud: Int) = {
        activeReplications --= activeReplications.filter(desc => desc.source == cloud || desc.target == cloud)
    }
}

