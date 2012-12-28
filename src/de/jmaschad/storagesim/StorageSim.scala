
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

object StorageSim {
  val MicroCloudCount = 4

  val UserCount = 0
  val RequestRatePerUser = 10
  val SimDuration = 10

  val bucketCountDist = new NormalDistribution(5, 2)
  val objectCountDist = new NormalDistribution(100, 20)
  val sizeDist = new NormalDistribution(10 * Units.MByte, 5 * Units.MByte)

  def main(args: Array[String]) {
    CloudSim.init(1, Calendar.getInstance(), false)

    val disposer = new Disposer("dp", RequestDistributor.randomRequestDistributor())
    CloudSim.send(0, disposer.getId(), SimDuration + 1, Disposer.Shutdown, null)

    val clouds = genMicroClouds(disposer)
    val users = genUsers(disposer)

    CloudSim.startSimulation();
  }

  private def genMicroClouds(disposer: de.jmaschad.storagesim.model.Disposer): Seq[MicroCloud] =
    for (i <- 1 to MicroCloudCount) yield {
      val storageDevices = for (i <- 1 to 10) yield new StorageDevice(capacity = 2 * Units.TByte, bandwidth = 600 * Units.MByte)
      val charact = new MicroCloudResourceCharacteristics(bandwidth = 125 * Units.MByte, storageDevices)

      new MicroCloud("mc" + i, charact, disposer)
    }

  private def genUsers(disposer: Disposer): Seq[User] =
    for (i <- 1 to UserCount) yield {
      val start = 3.0
      val user = new User("u" + i, disposer)
      val bucketCount = bucketCountDist.sample().intValue.min(1)
      val objectCount = objectCountDist.sample().intValue.min(1)

      val objects = genObjects(bucketCount, objectCount, sizeDist, user)
      user.addBehavior(Behavior.uniformTimeUniformObject(start, SimDuration, RequestRatePerUser, objects, obj => new GetObject(obj, user)))
      user
    }

  private def genObjects(bucketCount: Int, objectCount: Int, sizeDist: RealDistribution, user: User): IndexedSeq[StorageObject] = {
    assume(bucketCount > 0 && objectCount > 0)
    for (i <- 1 to objectCount) yield new StorageObject("bucket" + Random.nextInt(bucketCount), "obj" + i, sizeDist.sample())
  }
}