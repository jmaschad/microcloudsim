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

class RandomBucketBasedSelector(
    val log: String => Unit,
    val send: (Int, Int, Object) => Unit) extends CloudSelector {

    private var clouds = Set.empty[Int]

    private var bucketCloudMap = Map.empty[String, Set[Int]]
    private var cloudBucketMap = Map.empty[Int, Set[String]]

    private var objectCloudMap = Map.empty[StorageObject, Set[Int]]
    private var cloudObjectMap = Map.empty[Int, Set[StorageObject]]

    def initialize(storageObjects: Set[StorageObject]) {
        assert(clouds.nonEmpty)

        // distribute first copy
        storageObjects.foreach(obj => {
            val target = selectForPost(obj) match {
                case Right(t) => t
                case _ => throw new IllegalStateException
            }
            addObject(target, obj)
            send(target, MicroCloud.Initialize, obj)
        })

        // distribute replicas
        createBucketDistributionPlan().foreach(ots => {
            addObject(ots._2, ots._1)
            send(ots._2, MicroCloud.Initialize, ots._1)
        })
    }

    // for each bucket if a cloud with that bucket does not have all
    // objects, add a replication request
    private def createFillBucketPlan(): Iterable[(StorageObject, Int)] = {
        val maximumBucketMap = objectCloudMap.keySet.groupBy(_.bucket)
        val cloudBucketObjectMap = cloudObjectMap.map(co => co._1 -> co._2.groupBy(_.bucket))

        for (
            cloud <- cloudBucketObjectMap.keys;
            bucket <- cloudBucketObjectMap(cloud).keys;
            missingObject <- maximumBucketMap(bucket).diff(cloudBucketObjectMap(cloud)(bucket))
        ) yield {
            missingObject -> cloud
        }
    }

    private def createBucketDistributionPlan(): Iterable[(StorageObject, Int)] = {
        // for each bucket, if that bucket is not replicated enough, add 
        // a replication request
        val replicaCount = StorageSim.configuration.replicaCount
        val clouds = cloudBucketMap.keySet
        val bucketObjectMap = objectCloudMap.keySet.groupBy(_.bucket)
        val bucketsToReplicate = bucketCloudMap.filter(_._2.size < replicaCount)

        bucketsToReplicate.keys.flatMap(bucket => {
            val goalCount = replicaCount - bucketsToReplicate(bucket).size
            val possibleTargets = clouds -- bucketsToReplicate(bucket)
            val targets = distictRandomSelectN(goalCount, possibleTargets.toIndexedSeq)
            bucketObjectMap(bucket).flatMap(obj => targets.map(target => obj -> target))
        })
    }

    def addCloud(cloud: Int, status: Object) = status match {
        case CloudStatus(objects) =>
            // assert we don't know this cloud already
            assert(!clouds.contains(cloud))
            clouds += cloud

            assert(!cloudObjectMap.isDefinedAt(cloud))
            assert(!objectCloudMap.values.flatten.toSet.contains(cloud))
            assert(!bucketCloudMap.values.flatten.toSet.contains(cloud))
            assert(!cloudBucketMap.isDefinedAt(cloud))
            objects.foreach(addObject(cloud, _))

        case _ =>
            throw new IllegalStateException
    }

    def removeCloud(cloud: Int) = {

    }

    def processStatusMessage(cloud: Int, message: Object) =
        message match {
            case CloudStatus(objects) =>
                assert(clouds.contains(cloud))
                assert(cloudObjectMap.getOrElse(cloud, Set.empty) == objects)

            case AddedObject(storageObject) =>
                assert(clouds.contains(cloud))
                // assert we did not think this object was already there
                assert(!objectCloudMap.getOrElse(storageObject, Set.empty).contains(cloud))
                assert(!cloudObjectMap.getOrElse(cloud, Set.empty).contains(storageObject))
                addObject(cloud, storageObject)

            case _ => throw new IllegalStateException

        }

    private def addObject(cloud: Int, storageObject: StorageObject) = {
        val bucket = storageObject.bucket
        bucketCloudMap += bucket -> (bucketCloudMap.getOrElse(bucket, Set.empty) + cloud)
        cloudBucketMap += cloud -> (cloudBucketMap.getOrElse(cloud, Set.empty) + bucket)

        objectCloudMap += storageObject -> (objectCloudMap.getOrElse(storageObject, Set.empty) + cloud)
        cloudObjectMap += cloud -> (cloudObjectMap.getOrElse(cloud, Set.empty) + storageObject)
    }

    def selectForPost(storageObject: StorageObject): Either[RequestState, Int] =
        if (objectCloudMap.isDefinedAt(storageObject)) {
            Left(RequestState.ObjectExists)
        } else if (clouds.isEmpty) {
            Left(RequestState.NoOnlineClouds)
        } else if (bucketCloudMap.contains(storageObject.bucket)) {
            Right(randomSelect1(bucketCloudMap(storageObject.bucket).toIndexedSeq))
        } else {
            Right(randomSelect1(clouds.toIndexedSeq))
        }

    def selectForGet(storageObject: StorageObject): Either[RequestState, Int] =
        objectCloudMap.get(storageObject) match {
            case Some(clouds) => Right(randomSelect1(clouds.toIndexedSeq))
            case None =>
                Left(RequestState.ObjectNotFound)
        }

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