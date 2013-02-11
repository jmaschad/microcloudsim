package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.microcloud.Replicate
import org.cloudbus.cloudsim.core.SimEvent
import java.util.Objects
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.Replicate
import de.jmaschad.storagesim.model.microcloud.MicroCloud

class ReplicationController(
    log: String => Unit,
    send: (Int, Int, Object) => Unit,
    selector: CloudSelector) {

    private class ReplicationTracker(storageObjects: Set[StorageObject]) {
        val start = CloudSim.clock()
        var requests = storageObjects.groupBy(_.bucket).flatMap(b => {
            val target = selector.selectForPost(b._2) match {
                case Some(t) => t
                case None => throw new IllegalStateException
            }

            b._2.map(obj => selector.selectForGet(obj) match {
                case Some(source) => Replicate(source, target, obj)
                case None => throw new IllegalStateException
            })
        }).toSet

        // send initial replication requests
        sendRequests(requests)

        def requestSourceFailed(source: Int) = {
            val failedRequests = requests.filter(_.source == source)
            requests --= failedRequests

            val replacementRequests = failedRequests.map(req => {
                selector.selectForGet(req.storageObject) match {
                    case Some(source) => Replicate(source, req.target, req.storageObject)
                    case None => throw new IllegalStateException("No more sources for object available")
                }
            })
            requests ++= replacementRequests
            sendRequests(replacementRequests)
        }

        def requestTargetFailed(target: Int) = {
            val failedRequests = requests.filter(_.target == target)
            requests --= failedRequests

            val replacementRequests = failedRequests.groupBy(_.storageObject.bucket).flatMap(b => {
                val target = selector.selectForPost(b._2.map(_.storageObject)) match {
                    case Some(t) => t
                    case None => throw new IllegalStateException
                }

                b._2.map(req => Replicate(req.source, target, req.storageObject))
            }).toSet
            requests ++= replacementRequests
            sendRequests(replacementRequests)
        }

        def requestFinished(request: Replicate) = {
            requests -= request
            if (requests.isEmpty) {
                log("Offline cloud repaired [%.3f].".format(CloudSim.clock() - start))
                tracker -= this
            }
        }

        private def sendRequests(requests: Set[Replicate]) = {
            requests.foreach(req => send(req.source, MicroCloud.InterCloudRequest, req))
        }
    }

    private var tracker = Set.empty[ReplicationTracker]

    def repairOffline(storageObjects: Set[StorageObject]) = {
        tracker += new ReplicationTracker(storageObjects)
    }

    def requestFinished(request: Replicate) = tracker.foreach(_.requestFinished(request))

    def requestTargetFailed(target: Int) = tracker.foreach(_.requestTargetFailed(target))

    def requestSourceFailed(source: Int) = tracker.foreach(_.requestSourceFailed(source))
}

