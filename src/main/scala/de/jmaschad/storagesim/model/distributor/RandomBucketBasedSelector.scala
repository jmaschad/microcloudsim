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

    override protected def selectReplicationTarget(
        bucket: String,
        clouds: Set[Int],
        cloudLoad: Map[Int, Double],
        preselectedClouds: Set[Int]): Int = {
        val availableClouds = clouds diff preselectedClouds

        RandomUtils.randomSelect1(availableClouds.toIndexedSeq)
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