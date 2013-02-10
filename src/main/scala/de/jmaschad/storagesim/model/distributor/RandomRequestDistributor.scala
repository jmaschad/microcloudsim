package de.jmaschad.storagesim.model.distributor

import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.RequestType
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.Replicate

private[distributor] class RandomRequestDistributor(bucketObjectMap: Map[String, Set[StorageObject]]) extends RequestDistributor {
    private var cloudObjectMap = Map.empty[Int, Set[StorageObject]]
    private var objectMapping = Map.empty[StorageObject, Set[Int]]
    private var onlineClouds = Set.empty[Int]

    override def statusUpdate(onlineMicroClouds: Map[Int, Status]) = {
        cloudObjectMap = onlineMicroClouds.map(c => c._1 -> c._2.objects)
        objectMapping = onlineMicroClouds.toSeq.flatMap(m => m._2.objects.map(obj => obj -> m._1)).
            groupBy(_._1).mapValues(_.map(_._2).toSet)
        onlineClouds = onlineMicroClouds.keys.toSet
    }

    override def selectMicroCloud(request: Request): Option[Int] = onlineClouds.size match {
        case 0 => None
        case _ =>
            request.requestType match {
                case RequestType.Get => selectForLoad(request.storageObject)
                case RequestType.Put => selectForStore(request.storageObject)
                case _ => throw new IllegalStateException
            }
    }

    override def replicationRequests(): Set[Replicate] = {
        // replace missing objects
        val cloudRepair = cloudRepairRequests()
        // replicate all objects of died clouds
        val cloudReplace = cloudReplaceRequests()

        cloudRepair ++ cloudReplace
    }

    /*
     * Clouds should store complete copies of buckets. So we check
     * for each bucket the cloud has at least one object of, if there
     * are missing objects. 
     */
    private def cloudRepairRequests(): Set[Replicate] = {
        cloudObjectMap.flatMap(co => {
            val cloud = co._1
            val cloudBucketObjectMap = co._2.groupBy(_.bucket)
            val missingObjects = cloudBucketObjectMap.flatMap(bo => bucketObjectMap(bo._1) diff bo._2)
            missingObjects.map(obj => selectForLoad(obj) match {
                case Some(source) =>
                    Replicate(source, cloud, obj)
                case None =>
                    throw new IllegalStateException
            })
        }).toSet
    }

    private def replicationTargets(storageObject: StorageObject, count: Int): Set[Int] = {
        val possibleTargets = onlineClouds.diff(objectMapping(storageObject)).toSeq
        val possibleTargetCount = possibleTargets.size
        assert(possibleTargetCount >= count)

        possibleTargetCount match {
            case 1 => possibleTargets.toSet
            case n =>
                val dist = new UniformIntegerDistribution(0, n - 1)
                val uniqueSample = scala.collection.mutable.Set.empty[Int]
                while (uniqueSample.size < count) {
                    uniqueSample += dist.sample()
                }

                uniqueSample.map(idx => possibleTargets(idx)).toSet
        }

    }

    private def selectForLoad(storageObject: StorageObject): Option[Int] = {
        val clouds = objectMapping.getOrElse(storageObject, Iterable()).toSeq
        clouds.size match {
            case 0 => None
            case 1 => Some(clouds.head)
            case n => Some(clouds(new UniformIntegerDistribution(0, n - 1).sample()))
        }
    }

    private def selectForStore(storageObject: StorageObject): Option[Int] = {
        val bucket = storageObject.bucket
        val clouds = objectMapping.filter(_._1.bucket == bucket).values.flatten.toSet
        clouds.size match {
            case 0 =>
                val cloud: Int = onlineClouds.size match {
                    case 1 => onlineClouds.head
                    case n => onlineClouds.toSeq(new UniformIntegerDistribution(0, n - 1).sample())
                }
                objectMapping += storageObject -> Set(cloud)
                Some(cloud)

            case 1 =>
                Some(clouds.head)

            case n =>
                Some(clouds.toIndexedSeq(new UniformIntegerDistribution(0, n - 1).sample()))
        }
    }
}