package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.NetworkDelay

trait CloudSelectionModel {
    def selectForGet(sourceRegion: Int, targets: IndexedSeq[Int]): Int
}

object LatencyBasedSelection extends CloudSelectionModel {
    override def selectForGet(sourceRegion: Int, targets: IndexedSeq[Int]): Int = {
        val latencyOrderedTargets = targets sortWith { (t1, t2) =>
            val e1 = Entity.entityForId(t1)
            val e2 = Entity.entityForId(t2)
            NetworkDelay.between(sourceRegion, e1.region) < NetworkDelay.between(sourceRegion, e2.region)
        }
        latencyOrderedTargets.head
    }
}