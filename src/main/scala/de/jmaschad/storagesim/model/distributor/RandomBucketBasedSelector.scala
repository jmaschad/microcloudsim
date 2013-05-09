package de.jmaschad.storagesim.model.distributor

import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.MicroCloud
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.Entity

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

    override def selectForGet(region: Int, storageObject: StorageObject): Either[RequestSummary, Int] =
        distributionState getOrElse (storageObject, Set.empty) match {
            case targets if targets.size == 0 =>
                Left(ObjectNotFound)
            case targets if targets.size == 1 =>
                Right(targets.head)
            case targets =>
                val target = targets.toIndexedSeq(new UniformIntegerDistribution(0, targets.size - 1).sample())
                Right(target)
        }
}