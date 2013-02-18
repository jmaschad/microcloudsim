
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
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.user.UserBehavior
import de.jmaschad.storagesim.model.user.RequestType
import de.jmaschad.storagesim.model.processing.Downloader
import de.jmaschad.storagesim.model.processing.Transfer
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.Get

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

        log("create distributor")
        val distributor = new Distributor("dp")

        log("create clouds")
        val initialClouds = createMicroClouds(distributor)

        log("create objects")
        val initialObjects = createObjects()

        log("initialize the distributor with clouds and objects")
        distributor.initialize(initialClouds, initialObjects)

        log("create users")
        val users = createUsers(distributor)

        log("add user behavior")
        addBehavior(users, initialObjects)

        (1 to 2).map(i => {
            CloudSim.send(0, initialClouds.take(i).last.getId(), 10 + 2 * i, MicroCloud.Kill, null)
        })

        log("will start simulation")
        CloudSim.terminateSimulation(config.simDuration)
        CloudSim.startSimulation();

    }

    private def createObjects(): Set[StorageObject] = {
        val buckets = (1 to configuration.bucketCount).map("bucket-" + _)
        val objectCountDist = IntegerDistributionConfiguration.toDist(configuration.objectCountDistribution)
        val objectSizeDist = RealDistributionConfiguration.toDist(configuration.objectSizeDistribution)
        buckets.flatMap(bucket => {
            (1 to objectCountDist.sample()).map(i => new StorageObject("obj" + i, bucket, objectSizeDist.sample())).toSet
        }).toSet
    }

    private def createMicroClouds(disposer: Distributor): Set[MicroCloud] = {
        assert(configuration.cloudCount >= configuration.replicaCount)

        val cloudBandwidthDist = RealDistributionConfiguration.toDist(configuration.cloudBandwidthDistribution)
        (for (i <- 0 until configuration.cloudCount) yield {
            val storageDevices = for (i <- 1 to configuration.storageDevicesPerCloud)
                yield new StorageDevice(bandwidth = 600 * Units.MByte, capacity = 2 * Units.TByte)
            val charact = new ResourceCharacteristics(bandwidth = cloudBandwidthDist.sample().max(1), storageDevices = storageDevices)
            val failureBehavior = new MicroCloudFailureBehavior(
                RealDistributionConfiguration.toDist(configuration.cloudFailureDistribution),
                RealDistributionConfiguration.toDist(configuration.cloudRepairDistribution),
                RealDistributionConfiguration.toDist(configuration.diskFailureDistribution),
                RealDistributionConfiguration.toDist(configuration.diskRepairDistribution))

            new MicroCloud("mc" + (i + 1), charact, failureBehavior, disposer)
        }).toSet
    }

    private def createUsers(distributor: Distributor): Seq[User] =
        for (i <- 1 to configuration.userCount) yield {
            val storage = new StorageDevice(bandwidth = 600 * Units.MByte, capacity = 2 * Units.TByte)
            val resources = new ResourceCharacteristics(bandwidth = 12.5 * Units.MByte, storageDevices = Seq(storage))
            new User("u" + i, resources, Seq.empty[StorageObject], distributor)
        }

    private def addBehavior(users: Seq[User], storageObjects: Set[StorageObject]) = {
        val bucketMap = storageObjects.groupBy(_.bucket)
        val bucketSeq = bucketMap.keys.toIndexedSeq
        val bucketCount = bucketMap.keySet.size
        val bucketCountDist = IntegerDistributionConfiguration.toDist(configuration.accessedBucketCountDist)

        users.foreach(user => {
            val userBuckets = if (bucketCount == 1) {
                bucketMap.keySet
            } else {
                val userBucketCount = bucketCountDist.sample().max(1)
                new UniformIntegerDistribution(0, bucketCount - 1).sample(userBucketCount).map(bucketSeq(_)).toSet
            }

            val objects = userBuckets.flatMap(bucketMap(_)).toIndexedSeq
            configuration.behaviors.foreach(config => {
                val requestType = config.requestType
                val delayModel = RealDistributionConfiguration.toDist(config.delayModel)
                val objectSelectionModel = ObjectSelectionModel.toDist(objects.size, config.objectSelectionModel)

                val behavior = requestType match {
                    case RequestType.Get =>
                        UserBehavior(delayModel, objectSelectionModel, objects, Get({ Transfer.transferId() }, _))

                    case _ => throw new IllegalStateException
                }

                user.addBehavior(behavior)
            })
        })

    }

}