package de.jmaschad.storagesim.model.disposer

import java.util.Objects

import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.model.request.Request

object RequestDistributor {
    def randomRequestDistributor(): RequestDistributor = new RandomRequestDistributor
}

trait RequestDistributor {
    def statusUpdate(onlineMicroClouds: collection.Map[Int, Status])
    def selectMicroCloud(request: Request): Option[Int]
    def replicationRequests: Seq[(Int, ReplicationRequest)]
}
