package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.microcloud.Status

class StatusTracker {
    val MaxChecks = 2
    var trackedClouds = Map.empty[Int, Int]
    var cloudStatus = Map.empty[Int, Status]

    def onlineClouds: Map[Int, Status] = cloudStatus

    def online(microCloud: Int, status: Status): Unit = {
        val missedCount = trackedClouds.getOrElse(microCloud, 0) - 1
        trackedClouds = (trackedClouds - microCloud) + (microCloud -> missedCount.max(0))
        cloudStatus += microCloud -> status
    }

    def offline(microCloud: Int): Unit = {
        trackedClouds -= microCloud
    }

    def check(): Iterable[Int] = {
        trackedClouds = trackedClouds.mapValues(_ + 1)

        val offline = trackedClouds.filter(_._2 >= MaxChecks).keys
        trackedClouds --= offline
        cloudStatus --= offline
        offline
    }
}