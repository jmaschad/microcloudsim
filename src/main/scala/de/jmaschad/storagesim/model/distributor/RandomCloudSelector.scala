package de.jmaschad.storagesim.model.distributor

import scala.collection.mutable
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.RequestType
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.Replicate

private[distributor] class RandomCloudSelector extends CloudSelector {
    private var objectMapping = Map.empty[StorageObject, Set[Int]]
    private var onlineClouds = Set.empty[Int]

    override def statusUpdate(onlineMicroClouds: collection.Map[Int, Status]) = {
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
        val replicationNeeded = objectMapping.filter(_._2.size < StorageSim.replicaCount)

        replicationNeeded.flatMap(m => {
            val source = m._2.size match {
                case 1 => m._2.head
                case n => m._2.toSeq(new UniformIntegerDistribution(0, n - 1).sample())
            }
            val obj = m._1
            val count = StorageSim.replicaCount - m._2.size
            val targets = replicationTargets(obj, count)

            targets.map(t => Replicate(source, t, obj))
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
                val uniqueSample = mutable.Set.empty[Int]
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