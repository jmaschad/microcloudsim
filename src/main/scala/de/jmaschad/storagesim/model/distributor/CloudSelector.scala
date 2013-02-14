package de.jmaschad.storagesim.model.distributor

import java.util.Objects
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.user.RequestState._
import de.jmaschad.storagesim.model.microcloud.MicroCloud

trait CloudSelector {
    val send: (Int, Int, Object) => Unit
    val log: String => Unit

    /**
     * Create the initial configuration of the selector and micro clouds
     */
    def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject])

    /**
     * If a MicroCloud comes on-line this method should
     * be called to update the ClodSelector. It returns
     * the StorageObjects which must be replicated to
     * this cloud in order to fulfill replication constraints.
     */
    def addCloud(cloud: Int, status: Object)

    /**
     * If a MicroCloud goes off-line this method should
     * be called to update the ClodSelector. It returns
     * the StorageObjects which must be replicated to
     * restore given replication objectives
     */
    def removeCloud(cloud: Int)

    /**
     * MicroClouds send status updates which can be used
     * by CloudSelectors to select appropriate clouds
     * for future requests.
     */
    def processStatusMessage(cloud: Int, message: Object)

    /**
     * Returns a cloud id or a POST request, if the request
     * is satisfiable or None otherwise.
     */
    def selectForPost(storageObjects: StorageObject): Either[RequestState, Int]

    /**
     * Returns a cloud id or a GET request, if the request
     * is satisfiable or None otherwise.
     */
    def selectForGet(storageObject: StorageObject): Either[RequestState, Int]
}