package de.jmaschad.storagesim.model.distributor

import java.util.Objects
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.microcloud.Replicate
import de.jmaschad.storagesim.model.processing.StorageObject

object CloudSelector {
    def randomRequestDistributor(log: String => Unit, sendNow: (Int, Int, Object) => Unit): CloudSelector = new RandomCloudSelector(log, sendNow)
}

trait CloudSelector {
    def statusUpdate(onlineMicroClouds: Map[Int, Status])
    def selectMicroCloud(request: Request): Option[Int]
    def repairOfflineCloud(cloud: Int)
    def selectForPost(storageObjects: Set[StorageObject]): Option[Int]
    def selectForGet(storageObject: StorageObject): Option[Int]
}
