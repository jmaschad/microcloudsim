package de.jmaschad.storagesim.model.disposer

import scala.collection.mutable
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.RequestType
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.StorageSim

private[disposer] class RandomRequestDistributor extends RequestDistributor {
    private var bucketMapping = Map.empty[String, Set[Int]]
    private var onlineClouds = Seq.empty[Int]

    override def statusUpdate(onlineMicroClouds: collection.Map[Int, Status]) = {
        bucketMapping = onlineMicroClouds.toSeq.flatMap(m => m._2.buckets.map(bucket => bucket -> m._1)).
            groupBy(_._1).mapValues(_.map(_._2).toSet)
        onlineClouds = onlineMicroClouds.keys.toSeq
    }

    override def selectMicroCloud(request: Request): Option[Int] = onlineClouds.size match {
        case 0 => None
        case _ =>
            val cloudsForBucket = bucketMapping.getOrElse(request.storageObject.bucket, Iterable()).toSeq
            request.requestType match {
                case RequestType.Get => selectForLoad(request.storageObject)
                case RequestType.Put => selectForStore(request.storageObject)
                case _ => throw new IllegalStateException
            }
    }

    override def replicationRequests(): Seq[(Int, ReplicationRequest)] = {
        val replicationNeeded = bucketMapping.filter(_._2.size < StorageSim.replicaCount)

        replicationNeeded.toSeq.map(bucketMapping => {
            val replicationSource = bucketMapping._2.head
            val bucket = bucketMapping._1
            val count = StorageSim.replicaCount - bucketMapping._2.size
            replicationSource -> new ReplicationRequest(replicationTargets(bucketMapping._1, count), bucketMapping._1)
        })
    }

    private def replicationTargets(bucket: String, count: Int): Iterable[Int] = {
        val possibleTargets = onlineClouds.diff(bucketMapping(bucket).toSeq)
        val possibleTargetCount = possibleTargets.size
        assert(possibleTargetCount >= count)

        possibleTargetCount match {
            case 1 => possibleTargets
            case n =>
                val dist = new UniformIntegerDistribution(0, possibleTargetCount - 1)
                val uniqueSample = mutable.Set.empty[Int]
                while (uniqueSample.size < count) {
                    uniqueSample += dist.sample()
                }

                uniqueSample.map(idx => possibleTargets(idx))
        }

    }

    private def selectForLoad(storageObject: StorageObject): Option[Int] = {
        val bucket = storageObject.bucket
        val clouds = bucketMapping.getOrElse(bucket, Iterable()).toSeq
        clouds.size match {
            case 0 => None
            case 1 => Some(clouds.head)
            case n => Some(clouds(new UniformIntegerDistribution(0, n - 1).sample()))
        }
    }

    private def selectForStore(storageObject: StorageObject): Option[Int] = {
        val bucket = storageObject.bucket
        val clouds = bucketMapping.getOrElse(bucket, Iterable()).toSeq
        clouds.size match {
            case 0 =>
                val cloud = onlineClouds.size match {
                    case 1 => onlineClouds.head
                    case n => onlineClouds(new UniformIntegerDistribution(0, n - 1).sample())
                }
                bucketMapping += bucket -> Set(cloud)
                Some(cloud)

            case 1 =>
                Some(clouds.head)

            case n =>
                Some(clouds(new UniformIntegerDistribution(0, n - 1).sample()))
        }
    }
}