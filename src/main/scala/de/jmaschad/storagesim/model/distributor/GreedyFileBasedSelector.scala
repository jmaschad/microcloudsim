package de.jmaschad.storagesim.model.distributor

import scala.collection.immutable.Set
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.processing.StorageObject

class GreedyFileBasedSelector(log: String => Unit, dialogCenter: DialogCenter)
    extends AbstractFileBasedSelector(log, dialogCenter) {

    def createDistributionPlan(cloudIds: Set[Int],
        objects: Set[StorageObject],
        currentPlan: Map[StorageObject, Set[Int]] = Map.empty): Map[StorageObject, Set[Int]] = {
        Map.empty
    }
}