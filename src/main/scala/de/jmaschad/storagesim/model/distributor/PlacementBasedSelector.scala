package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.StorageObject
import scala.collection.immutable.Set
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary

class PlacementBasedSelector(log: String => Unit, dialogCenter: DialogEntity)
    extends AbstractFileBasedSelector(log, dialogCenter) {

    override protected def selectReplicas(
        obj: StorageObject,
        currentReplicas: Set[Int],
        clouds: Set[Int]): Set[Int] = Set.empty

    override def selectForGet(region: Int, storageObject: StorageObject): Int = -1

}