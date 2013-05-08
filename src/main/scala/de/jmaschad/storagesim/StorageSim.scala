
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
import org.apache.commons.math3.util.ArithmeticUtils

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

        setLogFile(experimentDir)
        try {
            run()
        } catch {
            case ex =>
                Log.line("SIMULATION", "Exited with exception:\n" + ex + "\n\n" + ex.getStackTraceString)
        }
        closeLogFile()
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

    private def setLogFile(dir: Path): Unit = {
        val logFile = dir.resolve("experiment_log.txt")
        Log.open(logFile)
    }

    private def closeLogFile(): Unit =
        Log.close()

    private def run(): Unit = {
        CloudSim.init(1, Calendar.getInstance(), false)

        val distributor = new Distributor("dp")
        log("created distributor")

        val clouds = createClouds(distributor)
        log("created " + clouds.size + " clouds")

        val objects = createObjects()
        log("created " + objects.size + " objects")

        val users = createUsers(distributor, objects)
        log("created " + users.size + " users")

        log("initialize the distributor with clouds and objects")
        distributor.initialize(clouds, objects, users.toSet)

        log("inititalize network latency")
        val topologyStream = Source.fromInputStream(getClass.getResourceAsStream("50areas_ba.brite"))
        val topologyFile = Files.createTempFile("topology", "brite")
        val writer = Files.newBufferedWriter(topologyFile, Charset.defaultCharset())
        topologyStream foreach { writer.write(_) }
        writer.close()
        NetworkTopology.buildNetworkTopology(topologyFile.toString())

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
        val bucketSampler = new UniformIntegerDistribution(0, buckets.size - 1)

        // all buckets get one fortune
        var bucketFortunes = IndexedSeq.empty[Int] ++ buckets.indices

        // one fourth gets one more
        (1 to buckets.size / 4) foreach { _ =>
            bucketFortunes = bucketFortunes :+ bucketSampler.sample()
        }

        // one sixteenth gets eight more
        (1 to buckets.size / 16) foreach { _ =>
            (1 to 8) foreach { _ =>
                bucketFortunes = bucketFortunes :+ bucketSampler.sample()
            }
        }

        // generate enough objects to use all possible placements
        val objectCount = ArithmeticUtils.binomialCoefficient(configuration.cloudCount, configuration.replicaCount) * 5
        val objectSizeDist = RealDistributionConfiguration.toDist(configuration.objectSize)

        // generate the objects and select the bucket by lot
        val bucketDrawer = new UniformIntegerDistribution(0, bucketFortunes.size - 1)
        (1L to objectCount) map { idx =>
            val objName = "obj" + idx
            val objSize = objectSizeDist.sample()
            val objBucket = buckets(bucketFortunes(bucketDrawer.sample()))
            new StorageObject(objName, objBucket, objSize)
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