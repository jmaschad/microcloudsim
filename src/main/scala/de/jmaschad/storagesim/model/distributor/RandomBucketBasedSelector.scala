package de.jmaschad.storagesim.model.distributor

import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.RandomUtils

class RandomBucketBasedSelector(log: String => Unit, dialogCenter: DialogCenter)
    extends AbstractBucketBasedSelector(log, dialogCenter) {

    override protected def selectReplicationTarget(
        bucket: String,
        clouds: Set[Int],
        cloudLoad: Map[Int, Double],
        preselectedClouds: Set[Int]): Int = {
        val availableClouds = clouds.diff(preselectedClouds)

        RandomUtils.randomSelect1(availableClouds.toIndexedSeq)
    }
}