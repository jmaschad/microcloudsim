package de.jmaschad.storagesim.model.distributor

import scala.collection.mutable
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.RequestType
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.Replicate
import de.jmaschad.storagesim.model.microcloud.CloudStatus
import de.jmaschad.storagesim.model.user.RequestState
import de.jmaschad.storagesim.model.user.RequestState._
import de.jmaschad.storagesim.model.microcloud.CloudStatus
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.AddedObject

class RandomBucketBasedSelector(
    val log: String => Unit,
    val send: (Int, Int, Object) => Unit) extends CloudSelector {

    private var objectCloudMap = Map.empty[StorageObject, Set[Int]]
    private var cloudObjectMap = Map.empty[Int, Set[StorageObject]]

    def initialize(storageObjects: Set[StorageObject]) {
        assert(cloudObjectMap.nonEmpty)

        val initialDistribution = storageObjects.map(obj => selectForPost(obj) match {
            case Right(cloud) => cloud -> obj
            case _ => throw new IllegalStateException()
        }).groupBy(_._1).map(c => c._1 -> c._2.map(_._2))

        initialDistribution.foreach(c => {
            send(c._1, MicroCloud.Initialize, c._2)
        })
    }

    def addCloud(cloud: Int, status: Object) = status match {
        case CloudStatus(objects) =>
            // assert we don't know this cloud already
            assert(!cloudObjectMap.isDefinedAt(cloud))
            assert(!objectCloudMap.values.flatten.toSet.contains(cloud))

            objects.foreach(obj => {
                objectCloudMap += obj -> (objectCloudMap.getOrElse(obj, Set.empty) + cloud)
            })
            cloudObjectMap += cloud -> objects

        case _ =>
            throw new IllegalStateException
    }

    def removeCloud(cloud: Int) = {

    }

    def processStatusMessage(cloud: Int, message: Object) =
        message match {
            case CloudStatus(objects) =>
                assert(cloudObjectMap(cloud) == objects)

            case AddedObject(storageObject) =>
                // assert we did not think this object was already there
                assert(!objectCloudMap.getOrElse(storageObject, Set.empty).contains(cloud))
                assert(!cloudObjectMap.getOrElse(cloud, Set.empty).contains(storageObject))

                objectCloudMap += storageObject -> (objectCloudMap.getOrElse(storageObject, Set.empty) + cloud)
                cloudObjectMap += cloud -> (cloudObjectMap.getOrElse(cloud, Set.empty) + storageObject)

            case _ => throw new IllegalStateException

        }

    def selectForPost(storageObject: StorageObject): Either[RequestState, Int] =
        if (objectCloudMap.isDefinedAt(storageObject)) {
            Left(RequestState.ObjectExists)
        } else if (cloudObjectMap.isEmpty) {
            Left(RequestState.NoOnlineClouds)
        } else {
            Right(selectRandom(cloudObjectMap.keys.toIndexedSeq))
        }

    def selectForGet(storageObject: StorageObject): Either[RequestState, Int] =
        objectCloudMap.get(storageObject) match {
            case Some(clouds) => Right(selectRandom(clouds.toIndexedSeq))
            case None =>
                Left(RequestState.ObjectNotFound)
        }

    private def selectRandom(values: IndexedSeq[Int]): Int = values.size match {
        case 1 => values.head
        case n if n > 1 => values(new UniformIntegerDistribution(0, n - 1).sample())
        case _ => throw new IllegalArgumentException
    }
}