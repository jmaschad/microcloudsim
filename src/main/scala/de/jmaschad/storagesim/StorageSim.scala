
package de.jmaschad.storagesim

import java.io.File
import scala.util.Random
import org.apache.commons.math3.distribution.IntegerDistribution
import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.cloudbus.cloudsim.core.CloudSim
import com.twitter.util.Eval
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
import de.jmaschad.storagesim.model.transfer.Transfer
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import org.cloudbus.cloudsim.NetworkTopology
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Calendar
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.nio.file.Files
import java.nio.charset.Charset
import java.io.PrintWriter

object StorageSim {
    private val log = Log.line("StorageSim", _: String)

    var configuration: StorageSimConfig = null

    def main(args: Array[String]) {
        configuration = args.size match {
            case 0 => new StorageSimConfig {}
            case 1 => Eval[StorageSimConfig](new File(args(0)))
            case _ => throw new IllegalArgumentException
        }

        val outDir = Paths.get(configuration.outputDir).toAbsolutePath().toRealPath()
        assert(Files.exists(outDir) && Files.isDirectory(outDir))
        val experimentDir = createExperimentDir(outDir)
        createDescription(experimentDir)

        (1 to configuration.passCount) foreach { pass =>
            println("pass " + pass)
            setLogFile(experimentDir, pass)
            run()
            closeLogFile()
        }
    }

    private def createExperimentDir(baseDir: Path): Path = {
        val date = DateTime.now()
        val fmt = DateTimeFormat.forPattern("dd.MM.yyyy HH:mm:ss")
        val dirName = fmt.print(date) + " " + configuration.selector.getClass().getSimpleName()
        val expDir = baseDir.resolve(dirName)

        assert(Files.notExists(expDir))
        Files.createDirectory(expDir)
    }

    private def createDescription(dir: Path): Unit = {
        val descFile = dir.resolve("description.txt")
        val descWriter = new PrintWriter(Files.newBufferedWriter(descFile, Charset.forName("UTF-8")))
        StorageSimConfig.printDescription(configuration, descWriter)
        descWriter.close()
    }

    private def setLogFile(dir: Path, pass: Int): Unit = {
        val logFile = dir.resolve("pass " + pass + " log.txt")
        Log.open(logFile)
    }

    private def closeLogFile(): Unit =
        Log.close()

    private def run(): Unit = {
        CloudSim.init(1, Calendar.getInstance(), false)

        log("create distributor")
        val distributor = new Distributor("dp")

        log("create clouds")
        val initialClouds = createClouds(distributor)

        log("create objects")
        val initialObjects = createObjects()

        log("create users")
        val users = createUsers(distributor, initialObjects)

        log("initialize the distributor with clouds and objects")
        distributor.initialize(initialClouds, initialObjects)

        log("inititalize network latency")
        val topologyFile = getClass.getResource("50areas_ba.brite").getPath().toString()
        NetworkTopology.buildNetworkTopology(topologyFile)

        log("schedule catastrophe")
        val nonEmptyClouds = initialClouds.filterNot(_.isEmpty)
        (1 to 1) map
            { i => CloudSim.send(0, nonEmptyClouds.take(i).last.getId(), 5 + i, MicroCloud.Kill, null) }

        log("will start simulation")
        CloudSim.terminateSimulation(configuration.simDuration)
        CloudSim.startSimulation();
    }

    private def createClouds(disposer: Distributor): Set[MicroCloud] = {
        assert(configuration.cloudCount >= configuration.replicaCount)

        val regionDist = new UniformIntegerDistribution(1, configuration.regionCount)
        val cloudBandwidthDist = RealDistributionConfiguration.toDist(configuration.cloudBandwidthDistribution)
        (0 until configuration.cloudCount) map { i: Int =>
            val storageDevices = (1 to configuration.storageDevicesPerCloud) map
                { _ => new StorageDevice(2 * Units.TByte) }
            val charact = new ResourceCharacteristics(bandwidth = cloudBandwidthDist.sample().max(1), storageDevices = storageDevices)
            new MicroCloud("mc" + (i + 1), regionDist.sample(), charact, disposer)
        } toSet
    }

    private def createObjects(): Set[StorageObject] = {
        val buckets = (1 to configuration.bucketCount) map { "bucket-" + _ }
        val objectCountDist = IntegerDistributionConfiguration.toDist(configuration.objectCountDistribution)
        val objectSizeDist = RealDistributionConfiguration.toDist(configuration.objectSizeDistribution)
        buckets flatMap { bucket: String =>
            (1 to objectCountDist.sample()) map { i: Int => new StorageObject("obj" + i, bucket, objectSizeDist.sample()) } toSet
        } toSet
    }

    private def createUsers(distributor: Distributor, storageObjects: Set[StorageObject]): Seq[User] = {
        val regionDist = new UniformIntegerDistribution(1, configuration.regionCount)
        val getDelayDist = RealDistributionConfiguration.toDist(configuration.medianGetDelayDistribution)

        val bucketMap = storageObjects.groupBy(_.bucket)
        val bucketSeq = bucketMap.keys.toIndexedSeq
        val bucketCount = bucketMap.keySet.size
        val bucketCountDist = IntegerDistributionConfiguration.toDist(configuration.accessedBucketCountDist)

        for (i <- 1 to configuration.userCount) yield {
            val userBuckets = if (bucketCount == 1) {
                bucketMap.keySet
            } else {
                val userBucketCount = bucketCountDist.sample().max(1)
                new UniformIntegerDistribution(0, bucketCount - 1).sample(userBucketCount) map { bucketSeq(_) } toSet
            }
            val objects = userBuckets flatMap { bucketMap(_) }
            val objectForGetDist = ObjectSelectionModel.toDist(objects.size, configuration.objectForGetDistribution)

            val storage = new StorageDevice(2 * Units.TByte)
            val resources = new ResourceCharacteristics(bandwidth = 4 * Units.MByte, storageDevices = Seq(storage))

            new User("u" + i, regionDist.sample(), objects, objectForGetDist, getDelayDist.sample() max 0.1, resources, distributor)
        }
    }
}