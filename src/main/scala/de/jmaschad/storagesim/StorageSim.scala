
package de.jmaschad.storagesim

import org.joda.time.format.DateTimeFormat
import java.io.PrintWriter
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.NetworkTopology
import de.jmaschad.storagesim.model.distributor.Distributor
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.MicroCloud
import java.nio.file.Paths
import de.jmaschad.storagesim.model.StorageObject
import java.nio.file.Path
import java.nio.file.Files
import org.joda.time.DateTime
import com.twitter.util.Eval
import scala.io.Source
import de.jmaschad.storagesim.model.user.User
import java.util.Calendar
import java.nio.charset.Charset
import java.io.File

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
        val clouds = createClouds(distributor)

        log("create objects")
        val objects = createObjects()

        log("create users")
        val users = createUsers(distributor, objects)

        log("initialize the distributor with clouds and objects")
        distributor.initialize(clouds, objects, users.toSet)

        log("inititalize network latency")
        val topologyStream = Source.fromInputStream(getClass.getResourceAsStream("50areas_ba.brite"))
        val topologyFile = Files.createTempFile("topology", "brite")
        val writer = Files.newBufferedWriter(topologyFile, Charset.defaultCharset())
        topologyStream foreach { writer.write(_) }
        writer.close()
        NetworkTopology.buildNetworkTopology(topologyFile.toString())

        log("schedule catastrophe")
        val nonEmptyClouds = clouds.filterNot(_.isEmpty)

        log("will start simulation")
        CloudSim.terminateSimulation(configuration.simDuration)
        CloudSim.startSimulation();
    }

    private def createClouds(disposer: Distributor): Set[MicroCloud] = {
        assert(configuration.cloudCount >= configuration.replicaCount)
        val cloudBandwidthDist = RealDistributionConfiguration.toDist(configuration.cloudBandwidth)

        (0 until configuration.cloudCount) map { i: Int =>
            val name = "mc" + (i + 1)
            val region = (i % (configuration.regionCount - 1)) + 1
            assert(region != 0)
            val bandwidth = cloudBandwidthDist.sample().max(1)
            new MicroCloud(name, region, bandwidth, disposer)
        } toSet
    }

    private def createObjects(): Set[StorageObject] = {
        val buckets = (1 to configuration.bucketCount) map { "bucket-" + _ }

        val bucketSizeType = IntegerDistributionConfiguration.toDist(configuration.bucketSizeType)
        val bucketSizes = Seq(30, 200, 1000)

        val objectSizeDist = RealDistributionConfiguration.toDist(configuration.objectSize)

        buckets flatMap { bucket: String =>
            val sizeTypeIdx = bucketSizeType.sample() - 1
            val objectCount = bucketSizes(sizeTypeIdx)

            (1 to objectCount) map { idx: Int =>
                val objSize = objectSizeDist.sample()
                new StorageObject("obj" + idx, bucket, objSize)
            } toSet
        } toSet
    }

    private def createUsers(distributor: Distributor, objects: Set[StorageObject]): Seq[User] = {

        val regionDist = new UniformIntegerDistribution(1, configuration.regionCount - 1)
        val meanGetIntervalDist = RealDistributionConfiguration.toDist(configuration.meanGetInterval)

        val bucketObjectMap = objects.groupBy(_.bucket)
        val bucketSeq = bucketObjectMap.keys.toIndexedSeq
        val bucketCount = bucketObjectMap.keySet.size
        val bucketsPerUserDist = IntegerDistributionConfiguration.toDist(configuration.bucketsPerUser)

        for (i <- 1 to configuration.userCount) yield {
            val userName = "u" + i

            val region = regionDist.sample()
            assert(region != 0)

            val userBuckets = if (bucketCount == 1) {
                bucketObjectMap.keySet
            } else {
                val userBucketCount = bucketsPerUserDist.sample() max 1
                new UniformIntegerDistribution(0, bucketCount - 1).sample(userBucketCount) map { bucketSeq(_) } toSet
            }
            val userObjects = userBuckets flatMap { bucketObjectMap(_) }

            val objectForGetDist = ObjectSelectionModel.toDist(userObjects.size, configuration.getTargetModel)

            val meanGetInterval = meanGetIntervalDist.sample() max 0.1

            val bandwidth = 4.0 * Units.MByte

            new User(userName, region, userObjects, objectForGetDist, meanGetInterval, bandwidth, distributor)
        }
    }
}