package de.jmaschad.storagesim.model.distribution

import de.jmaschad.storagesim.model.microcloud.MicroCloudStatus
import de.jmaschad.storagesim.model.UserRequest
import de.jmaschad.storagesim.model.UserObjectRequest
import org.apache.commons.math3.distribution.UniformIntegerDistribution

object RequestDistributor {
  def randomRequestDistributor(): RequestDistributor = new RandomRequestDistributor
}

trait RequestDistributor {
  def selectMicroCloud(request: UserObjectRequest, onlineMicroClouds: collection.Map[Int, MicroCloudStatus]): Int
}

private[distribution] class RandomRequestDistributor extends RequestDistributor {
  var bucketMapping = Map.empty[String, Int]

  def selectMicroCloud(request: UserObjectRequest, onlineMicroClouds: collection.Map[Int, MicroCloudStatus]): Int = {
    bucketMapping = bucketMapping.filter(bucketMap => onlineMicroClouds.keySet.contains(bucketMap._2))

    val bucket = request.storageObject.bucket
    if (!bucketMapping.contains(bucket)) {

      val cloud: Int = if (onlineMicroClouds.size == 1) {
        onlineMicroClouds.head._1
      } else {
        val dist = new UniformIntegerDistribution(0, onlineMicroClouds.size - 1)
        onlineMicroClouds.keys.toSeq(dist.sample())
      }

      bucketMapping += bucket -> cloud
      cloud
    } else {
      bucketMapping(bucket)
    }
  }
}