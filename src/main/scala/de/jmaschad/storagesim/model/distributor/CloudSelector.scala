package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.user.User

trait CloudSelector {
    /**
     * Create the initial configuration of the selector and micro clouds
     */
    def initialize(clouds: Set[MicroCloud], objects: Set[StorageObject], users: Set[User]): Unit

    /**
     * If a MicroCloud comes on-line this method should
     * be called to update the ClodSelector. It returns
     * the StorageObjects which must be replicated to
     * this cloud in order to fulfill replication constraints.
     */
    def addCloud(cloud: Int): Unit

    /**
     * If a MicroCloud goes off-line this method should
     * be called to update the ClodSelector. It returns
     * the StorageObjects which must be replicated to
     * restore given replication objectives
     */
    def removeCloud(cloud: Int): Unit

    /**
     * Allows the cloud to keep track of current downloads
     */
    def addedObject(cloud: Int, obj: StorageObject): Unit

    /**
     * Returns a cloud id or a GET request, if the request
     * is satisfiable or None otherwise.
     */
    def selectForGet(area: Int, storageObject: StorageObject): Either[RequestSummary, Int]
}