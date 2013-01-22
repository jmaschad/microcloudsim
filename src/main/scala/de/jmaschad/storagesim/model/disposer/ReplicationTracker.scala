package de.jmaschad.storagesim.model.disposer

import org.cloudbus.cloudsim.core.CloudSim

private[disposer] final class ReplicationTracker(private val log: String => Unit) {
    var dueReplications = Map.empty[String, Set[Int]]
    private var beginningOfRepair = Double.NegativeInfinity

    def dueReplicationCount = dueReplications.values.flatten.size

    def trackedReplicationRequests(requests: Set[ReplicationRequest]): Set[ReplicationRequest] = {
        if (dueReplications.isEmpty && requests.nonEmpty) {
            log("starting repair")
            assert(beginningOfRepair == Double.NegativeInfinity)
            beginningOfRepair = CloudSim.clock()
        }

        requests.flatMap(req => {
            val targets = req.targets.diff(dueReplications.getOrElse(req.bucket, Set.empty))
            if (targets.nonEmpty) {
                dueReplications += req.bucket -> (dueReplications.getOrElse(req.bucket, Set.empty) ++ targets)
                Some(ReplicationRequest(req.source, targets, req.bucket))
            } else {
                None
            }
        })
    }

    def replicationRequestCompleted(request: ReplicationRequest, cloud: Int) = {
        val bucket = request.bucket
        assert(dueReplications.contains(bucket))
        assert(dueReplications(bucket).contains(cloud))

        dueReplications += (bucket -> (dueReplications(bucket) - cloud))
        if (dueReplications(bucket).isEmpty) {
            dueReplications -= bucket
        }

        dueReplicationCount match {
            case 0 =>
                log("repair completed in %.3fs".format(CloudSim.clock() - beginningOfRepair))
                beginningOfRepair = Double.NegativeInfinity

            case n =>
                val dueDesc = dueReplications.filter(_._2.nonEmpty).keys.take(5).
                    map(bucket => bucket + " on " + dueReplications(bucket).map(CloudSim.getEntityName(_)).mkString(", "))
                log(n + " due replication requests. " + dueDesc.mkString(", "))
        }
    }

    def cloudsWentOflline(offlineClouds: Iterable[Int]) = {
        dueReplications =
            dueReplications.map(reqCloudsMapping =>
                (reqCloudsMapping._1 -> (dueReplications(reqCloudsMapping._1) -- offlineClouds)))
    }
}