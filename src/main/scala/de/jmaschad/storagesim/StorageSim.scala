
package de.jmaschad.storagesim

import java.io.File
import java.util.Calendar
import scala.util.Random
import org.apache.commons.math3.distribution.IntegerDistribution
import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.cloudbus.cloudsim.core.CloudSim
import com.twitter.util.Eval
import de.jmaschad.storagesim.model.ResourceCharacteristics
import de.jmaschad.storagesim.model.ResourceCharacteristics
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.distributor.RequestDistributor
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.MicroCloudFailureBehavior
import de.jmaschad.storagesim.model.processing.StorageDevice
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.user.RequestType._
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.user.UserBehavior
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.processing.Downloader

object StorageSim {
    private val log = Log.line("StorageSim", _: String)

    var replicaCount = 1

    def main(args: Array[String]) {
        CloudSim.init(1, Calendar.getInstance(), false)

        val config = args.size match {
            case 0 => new StorageSimConfig {}
            case 1 => Eval[StorageSimConfig](new File(args(0)))
            case _ => throw new IllegalArgumentException
        }

        replicaCount = config.replicaCount

        val distributor = RequestDistributor.randomRequestDistributor

        log("create disposer")
        val disposer = createDisposer(distributor)

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
        val clouds = createMicroClouds(config, bucketObjectsMap, disposer)

        log("will start simulation")
        CloudSim.terminateSimulation(config.simDuration)
        CloudSim.startSimulation();
    }

    private def createDisposer(distributor: RequestDistributor): Distributor = new Distributor("dp", distributor)

    private def createUsers(userCount: Int, disposer: Distributor): Seq[User] =
        for (i <- 1 to userCount) yield {
            val storage = new StorageDevice(bandwidth = 600 * Units.MByte, capacity = 2 * Units.TByte)
            val resources = new ResourceCharacteristics(bandwidth = 2 * Units.MByte, storageDevices = Seq(storage))
            new User("u" + i, resources, Seq.empty[StorageObject], disposer)
        }

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
                    UserBehavior(delayModel, objectSelectionModel, objects, Request.get(user, _, { Downloader.transferId() }))

                case Put =>
                    UserBehavior(delayModel, objectSelectionModel, objects, Request.put(user, _, { Downloader.transferId() }))

                case _ => throw new IllegalStateException
            }

            user.addBehavior(behavior)
        })

    private def createMicroClouds(config: StorageSimConfig, bucketObjectsMap: Map[String, Iterable[StorageObject]], disposer: Distributor): Seq[MicroCloud] = {
        assert(config.cloudCount >= replicaCount)
        val buckets = bucketObjectsMap.keys.toIndexedSeq
        val cloudForBucketDist = new UniformIntegerDistribution(0, config.cloudCount - 1)
        val bucketCloudMapping = (for (bucket <- buckets) yield {
            var indices = Set.empty[Int]
            while (indices.size < replicaCount) indices += cloudForBucketDist.sample()
            (bucket -> indices)
        }).map(bucketToIndices => bucketToIndices._2.map(_ -> bucketToIndices._1)).flatten.groupBy(_._1).map(idxMapping => (idxMapping._1 -> idxMapping._2.map(_._2)))

        val cloudBandwidthDist = RealDistributionConfiguration.toDist(config.cloudBandwidthDistribution)
        for (i <- 0 until config.cloudCount) yield {
            val storageDevices = for (i <- 1 to config.storageDevicesPerCloud)
                yield new StorageDevice(bandwidth = 600 * Units.MByte, capacity = 2 * Units.TByte)
            val charact = new ResourceCharacteristics(bandwidth = cloudBandwidthDist.sample().max(1), storageDevices = storageDevices)
            val objects = bucketCloudMapping(i).map(bucketObjectsMap(_)).flatten
            val failureBehavior = new MicroCloudFailureBehavior(
                RealDistributionConfiguration.toDist(config.cloudFailureDistribution),
                RealDistributionConfiguration.toDist(config.cloudRepairDistribution),
                RealDistributionConfiguration.toDist(config.diskFailureDistribution),
                RealDistributionConfiguration.toDist(config.diskRepairDistribution))

            new MicroCloud("mc" + (i + 1), charact, objects, failureBehavior, disposer)
        }
    }

}