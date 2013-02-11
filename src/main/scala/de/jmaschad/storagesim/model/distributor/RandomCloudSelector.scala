package de.jmaschad.storagesim.model.distributor

import scala.collection.mutable
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.RequestType
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.Replicate

private[distributor] class RandomCloudSelector(
    log: String => Unit,
    sendNow: (Int, Int, Object) => Unit) extends CloudSelector {

    private var objectMapping = Map.empty[StorageObject, Set[Int]]
    private var onlineClouds = Map.empty[Int, Status]
    private val replicationTracker = new ReplicationController(log, sendNow, this)

    override def statusUpdate(onlineMicroClouds: Map[Int, Status]) = {
        objectMapping = onlineMicroClouds.toSeq.flatMap(m => m._2.objects.map(obj => obj -> m._1)).
            groupBy(_._1).mapValues(_.map(_._2).toSet)
        onlineClouds = onlineMicroClouds
    }

    override def selectMicroCloud(request: Request): Option[Int] = onlineClouds.size match {
        case 0 => None
        case _ =>
            request.requestType match {
                case RequestType.Get => selectForGet(request.storageObject)
                case RequestType.Put => selectForPost(Set(request.storageObject))
                case _ => throw new IllegalStateException
            }
    }

    override def repairOfflineCloud(cloud: Int) = {
        replicationTracker.repairOffline(onlineClouds(cloud).objects)
    }

    override def selectForGet(storageObject: StorageObject): Option[Int] = {
        val clouds = objectMapping.getOrElse(storageObject, Iterable()).toSeq
        clouds.size match {
            case 0 => None
            case 1 => Some(clouds.head)
            case n => Some(clouds(new UniformIntegerDistribution(0, n - 1).sample()))
        }
    }

    override def selectForPost(storageObjects: Set[StorageObject]): Option[Int] = {
        val bucket = storageObjects.groupBy(_.bucket).keySet match {
            case buckets if buckets.size == 1 => buckets.head
            case _ => throw new IllegalStateException
        }

        val storingClouds = storageObjects.flatMap(objectMapping.get(_)).flatten
        val potentialClouds = onlineClouds.keySet -- storingClouds

        potentialClouds.size match {
            case 1 => Some(potentialClouds.head)

            case n if n > 0 =>
                val cloudSeq = potentialClouds.toIndexedSeq
                val dist = new UniformIntegerDistribution(0, cloudSeq.size - 1)
                Some(cloudSeq(dist.sample()))

            case _ => None
        }
    }
}