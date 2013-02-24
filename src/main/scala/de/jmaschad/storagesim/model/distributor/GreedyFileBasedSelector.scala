package de.jmaschad.storagesim.model.distributor

import scala.collection.immutable.Set
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.processing.StorageObject

class GreedyFileBasedSelector(log: String => Unit, dialogCenter: DialogCenter)
    extends AbstractFileBasedSelector(log, dialogCenter) {

    override protected def selectReplicationTargets(
        obj: StorageObject,
        count: Int,
        clouds: Set[Int],
        preselectedClouds: Set[Int]): Set[Int] = {
        Set.empty
    }
}