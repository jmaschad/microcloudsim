package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.user.User

abstract class AbstractFileBasedSelector(
    log: String => Unit,
    dialogEntity: DialogEntity) extends CloudSelector {

    def initialize(clouds: Set[MicroCloud], objects: Set[StorageObject], users: Set[User]): Unit =
        throw new IllegalStateException

    def addCloud(cloud: Int): Unit =
        throw new IllegalStateException

    def removeCloud(cloud: Int): Unit =
        throw new IllegalStateException

    def addedObject(cloud: Int, obj: StorageObject): Unit =
        throw new IllegalStateException

    def selectForPost(storageObjects: StorageObject): Either[RequestSummary, Int] =
        throw new IllegalStateException

    def selectForGet(area: Int, storageObject: StorageObject): Either[RequestSummary, Int] =
        throw new IllegalStateException

    protected def selectReplicationTarget(obj: StorageObject,
        clouds: Set[Int],
        cloudLoad: Map[Int, Double],
        preselectedClouds: Set[Int]): Int
}