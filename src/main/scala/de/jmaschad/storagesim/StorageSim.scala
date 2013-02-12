
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
import de.jmaschad.storagesim.model.distributor.CloudSelector
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.MicroCloudFailureBehavior
import de.jmaschad.storagesim.model.processing.StorageDevice
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.user.RequestType._
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.user.UserBehavior
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.processing.Downloader
import de.jmaschad.storagesim.model.processing.Transfer

object StorageSim {
    private val log = Log.line("StorageSim", _: String)

    var configuration: StorageSimConfig = null

    def main(args: Array[String]) {
        CloudSim.init(1, Calendar.getInstance(), false)

        val config = args.size match {
            case 0 => new StorageSimConfig {}
            case 1 => Eval[StorageSimConfig](new File(args(0)))
            case _ => throw new IllegalArgumentException
        }
        configuration = config

        log("create disposer")
        val distributor = new Distributor("dp")

        log("create users")
        val users = createUsers(distributor)

        log("create objects")
        val bucketCountDist = IntegerDistributionConfiguration.toDist(config.bucketCountDistribution)
        val objectCountDist = IntegerDistributionConfiguration.toDist(config.objectCountDistribution)
        val objectSizeDist = RealDistributionConfiguration.toDist(config.objectSizeDistribution)
        val objects = createObjects(bucketCountDist, objectCountDist, objectSizeDist, users)

        log("add user behavior")
        users.foreach(user => addBehavior(user, objects(user)))

        log("create clouds")
        val bucketObjectsMap = objects.values.flatten.groupBy(_.bucket)
        val clouds = createMicroClouds(bucketObjectsMap, distributor)

        // initial object distribution
        CloudSim.send(0, distributor.getId, configuration.SystemBootDelay * 0.8, Distributor.Initialize, objects.values.flatten)

        log("will start simulation")
        CloudSim.terminateSimulation(config.simDuration)
        CloudSim.startSimulation();

    }

    private def createUsers(distributor: Distributor): Seq[User] =
        for (i <- 1 to configuration.userCount) yield {
            val storage = new StorageDevice(bandwidth = 600 * Units.MByte, capacity = 2 * Units.TByte)
            val resources = new ResourceCharacteristics(bandwidth = 12.5 * Units.MByte, storageDevices = Seq(storage))
            new User("u" + i, resources, Seq.empty[StorageObject], distributor)
        }

    private def createObjects(bucketCountDist: IntegerDistribution,
        objectCountDist: IntegerDistribution,
        sizeDist: RealDistribution,
        users: Seq[User]): Map[User, IndexedSeq[StorageObject]] =

        users.map(user => {
            val bucketCount = bucketCountDist.sample().max(1)
            val objects = (1 to bucketCount).flatMap(bucketIndex => {
                val bucket = "bucket-" + user.getName + "-" + bucketIndex
                val objectCount = objectCountDist.sample().max(1)

                (1 to objectCount).map(i => new StorageObject("obj" + i, bucket,
                    sizeDist.sample().max(1 * Units.Byte))).toIndexedSeq
            })
            user -> objects
        }).toMap

    private def addBehavior(user: User, objects: IndexedSeq[StorageObject]) =
        configuration.behaviors.foreach(config => {
            val requestType = config.requestType
            val delayModel = RealDistributionConfiguration.toDist(config.delayModel)
            val objectSelectionModel = ObjectSelectionModel.toDist(objects.size, config.objectSelectionModel)

            val behavior = requestType match {
                case Get =>
                    UserBehavior(delayModel, objectSelectionModel, objects, Request.get(user, _, { Transfer.transferId() }))

                case Post =>
                    UserBehavior(delayModel, objectSelectionModel, objects, Request.put(user, _, { Transfer.transferId() }))

                case _ => throw new IllegalStateException
            }

            user.addBehavior(behavior)
        })

    private def createMicroClouds(bucketObjectsMap: Map[String, Iterable[StorageObject]], disposer: Distributor): Seq[MicroCloud] = {
        assert(configuration.cloudCount >= configuration.replicaCount)
        val buckets = bucketObjectsMap.keys.toIndexedSeq
        val cloudForBucketDist = new UniformIntegerDistribution(0, configuration.cloudCount - 1)
        val bucketCloudMapping = (for (bucket <- buckets) yield {
            var indices = Set.empty[Int]
            while (indices.size < configuration.replicaCount) indices += cloudForBucketDist.sample()
            (bucket -> indices)
        }).map(bucketToIndices => bucketToIndices._2.map(_ -> bucketToIndices._1)).flatten.groupBy(_._1).map(idxMapping => (idxMapping._1 -> idxMapping._2.map(_._2)))

        val cloudBandwidthDist = RealDistributionConfiguration.toDist(configuration.cloudBandwidthDistribution)
        for (i <- 0 until configuration.cloudCount) yield {
            val storageDevices = for (i <- 1 to configuration.storageDevicesPerCloud)
                yield new StorageDevice(bandwidth = 600 * Units.MByte, capacity = 2 * Units.TByte)
            val charact = new ResourceCharacteristics(bandwidth = cloudBandwidthDist.sample().max(1), storageDevices = storageDevices)
            val objects = bucketCloudMapping(i).map(bucketObjectsMap(_)).flatten
            val failureBehavior = new MicroCloudFailureBehavior(
                RealDistributionConfiguration.toDist(configuration.cloudFailureDistribution),
                RealDistributionConfiguration.toDist(configuration.cloudRepairDistribution),
                RealDistributionConfiguration.toDist(configuration.diskFailureDistribution),
                RealDistributionConfiguration.toDist(configuration.diskRepairDistribution))

            new MicroCloud("mc" + (i + 1), charact, failureBehavior, disposer)
        }
    }

}