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
import sun.org.mozilla.javascript.internal.ast.Yield
import de.jmaschad.storagesim.model.processing.StorageObject

class RandomBucketBasedSelector(
    val log: String => Unit,
    val send: (Int, Int, Object) => Unit) extends CloudSelector {

    override def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject]) = {

    }

    override def addCloud(cloud: Int, status: Object) = {

    }

    override def removeCloud(cloud: Int) = {

    }

    override def processStatusMessage(cloud: Int, message: Object) =
        message match {
            case CloudStatus(objects) =>

            case AddedObject(storageObject) =>

            case _ => throw new IllegalStateException

        }

    private def addObject(cloud: Int, storageObject: StorageObject) = {

    }

    override def selectForPost(storageObject: StorageObject): Either[RequestState, Int] =
        Left(RequestState.UnsufficientSpace)

    override def selectForGet(storageObject: StorageObject): Either[RequestState, Int] =
        Left(RequestState.ObjectNotFound)

    private def randomSelect1[T](values: IndexedSeq[T]): T = distictRandomSelectN(1, values).head

    private def distictRandomSelectN[T](count: Int, values: IndexedSeq[_ <: T]): Set[T] = {
        assert(count > 0)
        assert(count <= values.size)

        values.size match {
            case 1 => Set(values.head)

            case n =>
                val dist = new UniformIntegerDistribution(0, n - 1)
                var distinctSample = Set.empty[Int]
                while (distinctSample.size < count) {
                    distinctSample += dist.sample()
                }
                distinctSample.map(values(_))
        }
    }
}