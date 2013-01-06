
package de.jmaschad.storagesim

import scala.util.Random
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.distribution.RealDistribution
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.Disposer
import de.jmaschad.storagesim.model.User
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.MicroCloudResourceCharacteristics
import de.jmaschad.storagesim.model.storage.StorageDevice
import de.jmaschad.storagesim.model.storage.StorageObject
import java.util.Calendar
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.model.distribution.RequestDistributor
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.request.GetRequest

object StorageSim {

  def main(args: Array[String]) {
    CloudSim.init(1, Calendar.getInstance(), false)

    val simDuration = 10
    val distributor = RequestDistributor.randomRequestDistributor
    val userCount = 100
    val RequestRatePerUser = 1

    val cloudCount = 5
    val storageDevicePerCloud = 10

    val bucketCountDist = new NormalDistribution(10, 2)
    val objectCountDist = new NormalDistribution(100, 20)
    val objectSizeDist = new NormalDistribution(100 * Units.MByte, 20 * Units.MByte)

    val disposer = createDisposer(distributor, simDuration)
    val users = createUsers(userCount, disposer)
    val objects = createObjects(bucketCountDist, objectCountDist, objectSizeDist, users)

    val bucketObjectsMap = objects.values.flatten.groupBy(_.bucket)
    val clouds = createMicroClouds(cloudCount, storageDevicePerCloud, bucketObjectsMap, disposer)

    users.foreach(u =>
      u.addBehavior(
        Behavior.uniformTimeUniformObject(3.0, simDuration, RequestRatePerUser, objects(u), (obj, time) => new GetRequest(obj, time))))

    CloudSim.startSimulation();
  }

  private def createDisposer(distributor: RequestDistributor, simulationDuration: Double): Disposer = {
    val disposer = new Disposer("dp", distributor)
    CloudSim.send(0, disposer.getId(), simulationDuration + 1, Disposer.Shutdown, null)
    disposer
  }

  private def createUsers(userCount: Int, disposer: Disposer): Seq[User] =
    for (i <- 1 to userCount) yield new User("u" + i, disposer)

  private def createObjects(bucketCountDist: RealDistribution, objectCountDist: RealDistribution, sizeDist: RealDistribution, users: Seq[User]): Map[User, IndexedSeq[StorageObject]] =
    users.map(user => {
      val objectCount = objectCountDist.sample().toInt.max(1)
      val bucketCount = bucketCountDist.sample().toInt.max(1)

      def createObject(idx: Int) = new StorageObject(
        "obj" + idx,
        "bucket-%s-%d".format(user.getName, Random.nextInt(bucketCount)),
        sizeDist.sample().max(1 * Units.Byte),
        user)

      user -> (1 to objectCount).map(createObject).toIndexedSeq
    }).toMap

  private def createMicroClouds(cloudCount: Int, storageDeviceCount: Int, bucketObjectsMap: Map[String, Iterable[StorageObject]], disposer: Disposer): Seq[MicroCloud] = {
    val bucketCount = bucketObjectsMap.keys.size
    val bucketGroups = if (bucketCount % cloudCount == 0) {
      bucketObjectsMap.keySet.grouped(bucketCount / cloudCount).toSeq
    } else {
      val temp = bucketObjectsMap.keySet.grouped(bucketCount / cloudCount).toSeq
      val lastBucket = Seq(temp(temp.length - 1) ++ temp(temp.length - 2))
      temp.patch(temp.size - 2, lastBucket, 2)
    }

    assert(bucketGroups.size == cloudCount, "expected %d, found %d".format(cloudCount, bucketGroups.size))

    for (i <- 0 until cloudCount) yield {
      val storageDevices = for (i <- 1 to storageDeviceCount)
        yield new StorageDevice(capacity = 2 * Units.TByte, bandwidth = 600 * Units.MByte)
      val charact = new MicroCloudResourceCharacteristics(bandwidth = 125 * Units.MByte, storageDevices)

      new MicroCloud("mc" + (i + 1), charact, bucketObjectsMap.filterKeys(bucketGroups(i).contains(_)).values.flatten, disposer)
    }
  }

}