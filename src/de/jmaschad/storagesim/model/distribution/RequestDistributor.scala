package de.jmaschad.storagesim.model.distribution

import scala.collection.mutable

import org.apache.commons.math3.distribution.UniformIntegerDistribution

import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.model.request.GetRequest
import de.jmaschad.storagesim.model.request.PutRequest
import de.jmaschad.storagesim.model.request.Request
import de.jmaschad.storagesim.model.storage.StorageObject

object RequestDistributor {
  def randomRequestDistributor(): RequestDistributor = new RandomRequestDistributor
}

trait RequestDistributor {
  def statusUpdate(onlineMicroClouds: collection.Map[Int, Status])
  def selectMicroCloud(request: Request): Option[Int]
}

private[distribution] class RandomRequestDistributor extends RequestDistributor {
  private val bucketMapping = mutable.Map.empty[String, Set[Int]]
  private var onlineClouds = Seq.empty[Int]

  def statusUpdate(onlineMicroClouds: collection.Map[Int, Status]) {
    bucketMapping.clear()
    for (cloud <- onlineMicroClouds; bucket <- cloud._2.buckets) {
      bucketMapping(bucket) = bucketMapping.getOrElse(bucket, Set.empty[Int]) + cloud._1
    }

    onlineClouds = onlineMicroClouds.keys.toSeq
  }

  def selectMicroCloud(request: Request): Option[Int] = onlineClouds.size match {
    case 0 => None
    case _ =>
      val cloudsForBucket = bucketMapping.getOrElse(request.storageObject.bucket, Iterable()).toSeq
      request match {
        case req: GetRequest => getObject(req.storageObject, cloudsForBucket)
        case req: PutRequest => putObject(req.storageObject, cloudsForBucket)
        case _ => throw new IllegalStateException
      }
  }

  private def getObject(obj: StorageObject, clouds: Seq[Int]): Option[Int] = clouds.size match {
    case 0 => None
    case 1 => Some(clouds.head)
    case n => Some(clouds(new UniformIntegerDistribution(0, n - 1).sample()))
  }

  private def putObject(obj: StorageObject, clouds: Seq[Int]): Option[Int] = clouds.size match {
    case 0 =>
      val cloud = onlineClouds.size match {
        case 1 => onlineClouds.head
        case n => onlineClouds(new UniformIntegerDistribution(0, n - 1).sample())
      }
      bucketMapping += obj.bucket -> Set(cloud)
      Some(cloud)

    case 1 =>
      Some(clouds.head)

    case n =>
      Some(clouds(new UniformIntegerDistribution(0, n - 1).sample()))
  }

}