
package de.jmaschad.storagesim

import java.util.Calendar
import java.io.File

import scala.util.Random

import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.IntegerDistribution
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.distribution.ExponentialDistribution
import org.apache.commons.math3.distribution.UniformIntegerDistribution

import org.cloudbus.cloudsim.core.CloudSim

import com.twitter.util.Eval

import de.jmaschad.storagesim.model.disposer.Disposer
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.MicroCloudResourceCharacteristics
import de.jmaschad.storagesim.model.storage.StorageDevice
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.model.disposer.RequestDistributor
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.RequestType._

object StorageSim {
    private val log = Log.line("StorageSim", _: String)

    var simDuration = 0.0
    var replicaCount = 1

    def main(args: Array[String]) {
        CloudSim.init(1, Calendar.getInstance(), false)

        val config = args.size match {
            case 0 => new StorageSimConfig {}
            case 1 => Eval[StorageSimConfig](new File(args(0)))
            case _ => throw new IllegalArgumentException
        }

        simDuration = config.simDuration
        replicaCount = config.replicaCount

        val distributor = RequestDistributor.randomRequestDistributor

        log("create disposer")
        val disposer = createDisposer(distributor, config.simDuration)

        log("create users")
        val users = createUsers(config.userCount, disposer)

        log("create objects")
        val bucketCountDist = IntegerDistributionConfiguration.toDist(config.bucketCountDistribution)
        val objectCountDist = IntegerDistributionConfiguration.toDist(config.objectCountDistribution)
        val objectSizeDist = RealDistributionConfiguration.toDist(config.objectSizeDistribution)
        val objects = createObjects(bucketCountDist, objectCountDist, objectSizeDist, users)

        log("add user behavior")
        users.foreach(user => addBehavior(user, objects(user), config.behaviors))

        log("create clouds")
        val bucketObjectsMap = objects.values.flatten.groupBy(_.bucket)
        val clouds = createMicroClouds(config.cloudCount, config.storageDevicesPerCloud, bucketObjectsMap, disposer)

        for (idx <- 0 until 3)
            CloudSim.send(0, clouds(idx).getId(), 10.0 + (idx * 5), MicroCloud.Kill, null)

        log("will start simulation")
        CloudSim.startSimulation();
    }

    private def createDisposer(distributor: RequestDistributor, simulationDuration: Double): Disposer = {
        val disposer = new Disposer("dp", distributor)
        CloudSim.send(0, disposer.getId(), simulationDuration + 1, Disposer.Shutdown, null)
        disposer
    }

    private def createUsers(userCount: Int, disposer: Disposer): Seq[User] =
        for (i <- 1 to userCount) yield new User("u" + i, disposer)

    private def createObjects(bucketCountDist: IntegerDistribution, objectCountDist: IntegerDistribution, sizeDist: RealDistribution, users: Seq[User]): Map[User, IndexedSeq[StorageObject]] =
        users.map(user => {
            val objectCount = objectCountDist.sample().max(1)
            val bucketCount = bucketCountDist.sample().max(1)

            def createObject(idx: Int) = new StorageObject(
                "obj" + idx,
                "bucket-" + user.getName + "-" + Random.nextInt(bucketCount),
                sizeDist.sample().max(1 * Units.Byte))

            user -> (1 to objectCount).map(createObject).toIndexedSeq
        }).toMap

    private def addBehavior(user: User, objects: IndexedSeq[StorageObject], behaviorConfigs: Seq[BehaviorConfig]) =
        behaviorConfigs.foreach(config => {
            val requestType = config.requestType
            val delayModel = RealDistributionConfiguration.toDist(config.delayModel)
            val objectSelectionModel = ObjectSelectionModel.toDist(objects.size, config.objectSelectionModel)

            val behavior = requestType match {
                case Get =>
                    Behavior(delayModel, objectSelectionModel, objects, Request.get(user, _))

                case Put =>
                    Behavior(delayModel, objectSelectionModel, objects, Request.put(user, _))

                case _ => throw new IllegalStateException
            }

            user.addBehavior(behavior)
        })

    private def createMicroClouds(cloudCount: Int, storageDeviceCount: Int, bucketObjectsMap: Map[String, Iterable[StorageObject]], disposer: Disposer): Seq[MicroCloud] = {
        assert(cloudCount >= replicaCount)
        val buckets = bucketObjectsMap.keys.toIndexedSeq
        val dist = new UniformIntegerDistribution(0, cloudCount - 1)
        val groupBucketsMapping = (for (bucket <- buckets) yield {
            var indices = Set.empty[Int]
            while (indices.size < replicaCount) indices += dist.sample()
            (bucket -> indices)
        }).map(bucketToIndices => bucketToIndices._2.map(_ -> bucketToIndices._1)).flatten.groupBy(_._1).map(idxMapping => (idxMapping._1 -> idxMapping._2.map(_._2)))

        for (i <- 0 until cloudCount) yield {
            val storageDevices = for (i <- 1 to storageDeviceCount)
                yield new StorageDevice(capacity = 2 * Units.TByte, bandwidth = 600 * Units.MByte)
            val charact = new MicroCloudResourceCharacteristics(bandwidth = 125 * Units.MByte, storageDevices)
            val objects = groupBucketsMapping(i).map(bucketObjectsMap(_)).flatten

            new MicroCloud("mc" + (i + 1), charact, objects, disposer)
        }
    }

}