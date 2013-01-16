package de.jmaschad.storagesim.model.disposer

private[disposer] final class ReplicationTracker {
    private var dueReplications = Map.empty[ReplicationRequest, Set[Int]]

    def trackedReplicationRequests(requests: Seq[(Int, ReplicationRequest)]): Seq[(Int, ReplicationRequest)] = {
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
    }

    def cloudsWentOflline(offlineClouds: Iterable[Int]) = {
        dueReplications =
            dueReplications.map(reqCloudsMapping =>
                (reqCloudsMapping._1 -> (dueReplications(reqCloudsMapping._1) -- offlineClouds)))
    }
}