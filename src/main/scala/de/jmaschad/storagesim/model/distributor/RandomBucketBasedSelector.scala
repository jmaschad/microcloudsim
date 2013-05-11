package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._

class RandomBucketBasedSelector(log: String => Unit, dialogEntity: DialogEntity)
    extends AbstractBucketBasedSelector(log, dialogEntity) {

    override protected def selectReplicas(
        bucket: String,
        currentReplicas: Set[Int],
        clouds: Set[Int]): Set[Int] =
        StorageSim.configuration.replicaCount - currentReplicas.size match {
            case 0 =>
                currentReplicas
            case n =>
                // select a new replication targets
                val availableClouds = { { clouds -- currentReplicas } toIndexedSeq }
                assert(availableClouds.size >= n)

                currentReplicas ++ RandomUtils.distinctRandomSelectN(n, availableClouds)
        }

    override def selectForGet(region: Int, storageObject: StorageObject): Int = {
        val possibleTargets = distributionState(storageObject).toIndexedSeq
        LatencyBasedSelection.selectForGet(region, possibleTargets)
    }
}