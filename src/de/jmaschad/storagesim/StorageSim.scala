
package de.jmaschad.storagesim

import scala.util.Random
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.distribution.RealDistribution
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.Disposer
import de.jmaschad.storagesim.model.GetObject
import de.jmaschad.storagesim.model.User
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.MicroCloudResourceCharacteristics
import de.jmaschad.storagesim.model.storage.StorageDevice
import de.jmaschad.storagesim.model.storage.StorageObject
import java.util.Calendar
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.model.GetObject
import de.jmaschad.storagesim.model.distribution.RequestDistributor
import de.jmaschad.storagesim.model.PutObject
import de.jmaschad.storagesim.model.PutObject

object StorageSim {
  val MicroCloudCount = 4

  val UserCount = 10
  val RequestRatePerUser = 10
  val SimDuration = 10

  val bucketCountDist = new NormalDistribution(5, 2)
  val objectCountDist = new NormalDistribution(100, 20)
  val sizeDist = new NormalDistribution(10 * Units.MByte, 5 * Units.MByte)

  def main(args: Array[String]) {
    CloudSim.init(1, Calendar.getInstance(), false)

    val distributor = RequestDistributor.randomRequestDistributor()
    val disposer = new Disposer("dp", distributor)
    CloudSim.send(0, disposer.getId(), SimDuration + 1, Disposer.Shutdown, null)

    val clouds = genMicroClouds(disposer)
    val users = genUsers(disposer)

    val objects = genObjects(users, bucketCountDist, objectCountDist, sizeDist)
    initCloudsWithObjects(distributor, clouds, objects)

    users.foreach(u =>
      u.addBehavior(
        Behavior.uniformTimeUniformObject(3.0, SimDuration, RequestRatePerUser, objects(u), obj => new GetObject(obj, u))))

    CloudSim.startSimulation();
  }

  private def genMicroClouds(disposer: de.jmaschad.storagesim.model.Disposer): Seq[MicroCloud] =
    for (i <- 1 to MicroCloudCount) yield {
      val storageDevices = for (i <- 1 to 10) yield new StorageDevice(capacity = 2 * Units.TByte, bandwidth = 600 * Units.MByte)
      val charact = new MicroCloudResourceCharacteristics(bandwidth = 125 * Units.MByte, storageDevices)

      new MicroCloud("mc" + i, charact, disposer)
    }

  private def genUsers(disposer: Disposer): Seq[User] =
    for (i <- 1 to UserCount) yield new User("u" + i, disposer)

  private def genObjects(users: Seq[User], bucketCountDist: RealDistribution, objectCountDist: RealDistribution, sizeDist: RealDistribution): Map[User, IndexedSeq[StorageObject]] = {
    val objectCount = objectCountDist.sample().toInt.max(1)
    val bucketCount = bucketCountDist.sample().toInt.max(1)

    val objectMappings = users.map(u => {
      val objects = (1 to objectCount).map(idx => new StorageObject("bucket" + Random.nextInt(bucketCount), "obj" + idx, sizeDist.sample()))
      u -> objects
    })

    Map(objectMappings: _*)
  }

  private def initCloudsWithObjects(distributor: RequestDistributor, clouds: Seq[MicroCloud], objects: Map[User, IndexedSeq[StorageObject]]) = {
    val statusMap = Map(clouds.map(c => c.getId() -> c.status): _*)
    val objectCloudMappings = for (
      userObjectMap <- objects;
      obj <- userObjectMap._2
    ) {
      val cloud = clouds.find(_.getId() == distributor.selectMicroCloud(PutObject(obj, userObjectMap._1), statusMap))
      cloud.getOrElse(throw new IllegalStateException).storeObject(obj)
    }
  }
}