package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.microcloud.Replicate
import org.cloudbus.cloudsim.core.SimEvent
import java.util.Objects
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.Replicate
import de.jmaschad.storagesim.model.microcloud.MicroCloud

class ReplicationController(
    send: (Int, Int, Object) => Unit,
    selector: CloudSelector) {

    private class ReplicationTracker(storageObjects: Set[StorageObject]) {
        val start = CloudSim.clock()
        val requests = storageObjects.groupBy(_.bucket).flatMap(b => {
            val target = selector.selectForPost(b._2) match {
                case Some(t) => t
                case None => throw new IllegalStateException
            }

            b._2.map(obj => selector.selectForGet(obj) match {
                case Some(source) => Replicate(source, target, obj)
                case None => throw new IllegalStateException
            })
        })

        // send replication requests
        requests.foreach(req => send(req.source, MicroCloud.InterCloudRequest, req))

        def requestSourceFailed(source: Int) = {}

        def requestTargetFailed(target: Int) = {}

        def requestFinished(request: Replicate) = {}
    }

    private var tracker = Set.empty[ReplicationTracker]

    def repairOffline(storageObjects: Set[StorageObject]) = {
        tracker += new ReplicationTracker(storageObjects)
    }

    def requestFinished(request: Replicate) = tracker.foreach(_.requestFinished(request))

    def requestTargetFailed(target: Int) = tracker.foreach(_.requestTargetFailed(target))

    def requestSourceFailed(source: Int) = tracker.foreach(_.requestSourceFailed(source))
}

