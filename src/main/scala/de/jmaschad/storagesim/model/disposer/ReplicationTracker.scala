package de.jmaschad.storagesim.model.disposer

import org.cloudbus.cloudsim.core.CloudSim

private[disposer] final class ReplicationTracker(private val log: String => Unit) {
    var dueReplications = Map.empty[ReplicationRequest, Set[Int]]
    private var beginningOfRepair = Double.NegativeInfinity

    def dueReplicationCount = dueReplications.values.flatten.size

    def trackedReplicationRequests(requests: Seq[(Int, ReplicationRequest)]): Seq[(Int, ReplicationRequest)] = {
        if (dueReplications.isEmpty && requests.nonEmpty) {
            log("starting repair")
            assert(beginningOfRepair == Double.NegativeInfinity)
            beginningOfRepair = CloudSim.clock()
        }

        val newRequests = requests.filter(req => !dueReplications.contains(req._2))
        dueReplications ++= newRequests.map(req => req._2 -> req._2.targets.toSet)
        newRequests
    }

    def replicationRequestCompleted(request: ReplicationRequest, cloud: Int) = {
        assert(dueReplications.contains(request))
        assert(dueReplications(request).contains(cloud))

        dueReplications = dueReplications +
            (request -> (dueReplications(request) - cloud))
        if (dueReplications(request).isEmpty) {
            dueReplications -= request
        }

        if (dueReplicationCount == 0) {
            log("repair completed in %.3fs".format(CloudSim.clock() - beginningOfRepair))
            beginningOfRepair = Double.NegativeInfinity
        }
    }

    def cloudsWentOflline(offlineClouds: Iterable[Int]) = {
        dueReplications =
            dueReplications.map(reqCloudsMapping =>
                (reqCloudsMapping._1 -> (dueReplications(reqCloudsMapping._1) -- offlineClouds)))
    }
}