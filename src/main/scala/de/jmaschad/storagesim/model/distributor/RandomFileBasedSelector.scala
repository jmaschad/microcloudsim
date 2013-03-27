package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck

class RandomFileBasedSelector(log: String => Unit, dialogCenter: DialogCenter)
    extends AbstractFileBasedSelector(log, dialogCenter) {

    override protected def selectReplicationTarget(
        obj: StorageObject,
        clouds: Set[Int],
        cloudLoad: Map[Int, Double],
        preselectedClouds: Set[Int]): Int = {
        RandomUtils.randomSelect1({ clouds diff preselectedClouds } toIndexedSeq)
    }
}