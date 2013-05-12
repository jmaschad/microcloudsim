
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
import org.apache.commons.math3.distribution.UniformRealDistribution
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

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

        setLogFile(outDir)
        StorageSimConfig.logDescription(configuration)

        try {
            run()
        } catch {
            case ex =>
                Log.line("SIMULATION", "Exited with exception:\n" + ex + "\n\n" + ex.getStackTraceString)
        }
        closeLogFile()
    }

    private def setLogFile(dir: Path): Unit = {
        val fmt = DateTimeFormat.forPattern("dd.MM.yyyy_HH:mm:ss")
        val date = fmt.print(DateTime.now) + ".%d".format(System.currentTimeMillis() % 1000)
        val logName = date + "_" + configuration.selector.getClass().getSimpleName() + ".txt"

        val logFile = dir.resolve(logName.toLowerCase)
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

        val users = createUsers(distributor, objects.toIndexedSeq)
        log("created " + users.size + " users")

        val objCounts = users.foldLeft(Map.empty[StorageObject, Int]) {
            case (objCounter, user) =>
                objCounter ++ { user.objects map { obj => obj -> { objCounter.getOrElse(obj, 0) + 1 } } }
        }
        val ucs = new DescriptiveStatistics(objCounts.values map { _.toDouble } toArray)
        val ocs = new DescriptiveStatistics(users map { _.objects.size.toDouble } toArray)
        val oss = new DescriptiveStatistics(users flatMap { _.objects map { _.size } } toArray)
        log("%d active objects. users/object: MIN %.0f MAX %.0f MEAN %.2f. objects/users: MIN %.0f MAX %.0f MEAN %.2f. object sizes MEAN %.2f STD.DEV %.2f".
            format(ucs.getN(), ucs.getMin, ucs.getMax, ucs.getMean, ocs.getMin, ocs.getMax, ocs.getMean, oss.getMean(), oss.getStandardDeviation()))

        log("initialize the distributor with clouds and objects")
        distributor.initialize(clouds, objects, users.toSet)

        log("inititalize network latency")
        val topologyStream = Source.fromInputStream(getClass.getResourceAsStream("50areas_ba.brite"))
        val topologyFile = Files.createTempFile("topology", "brite")
        val writer = Files.newBufferedWriter(topologyFile, Charset.defaultCharset())
        topologyStream foreach { writer.write(_) }
        writer.close()
        NetworkTopology.buildNetworkTopology(topologyFile.toString())

        log("wakeing up the stats")
        StatsCentral.wakeup()

        // controlled failure
        val failingCloud = clouds.toIndexedSeq(new UniformIntegerDistribution(0, clouds.size - 1).sample()).getId
        CloudSim.send(0, failingCloud, 30, MicroCloud.Kill, null)

        log("will start simulation")
        //        CloudSim.terminateSimulation(configuration.simDuration)
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

        // draw the bucket sizes 
        val bucketSizeDist = RealDistributionConfiguration.toDist(configuration.bucketSizeDist)
        val bucketSizes = bucketSizeDist.sample(buckets.size) map { _ / bucketSizeDist.getNumericalMean() }
        val bucketSizeSum = bucketSizes.sum

        // generate enough objects to use all possible placements
        val objectCount = (ArithmeticUtils.binomialCoefficient(configuration.cloudCount, configuration.replicaCount) * 5).toInt

        // draw object sizes
        val objectSizeDist = RealDistributionConfiguration.toDist(configuration.objectSize)
        val objectSizes = objectSizeDist.sample(objectCount)

        // generate the bucket allocation by lot
        val bucketDrawer = new UniformRealDistribution(0.0, bucketSizeSum)
        val bucketIndices = bucketDrawer.sample(objectCount) map { sample =>
            var cumulator = 0.0
            var index = -1
            while (cumulator < sample) {
                index += 1
                cumulator += bucketSizes(index)
            }
            index
        }

        // generate the objects
        (0 until objectCount) map { idx =>
            val objName = "obj" + (idx + 1)
            new StorageObject(objName, buckets(bucketIndices(idx)), objectSizes(idx))
        } toSet
    }

    private def createUsers(distributor: Distributor, objects: IndexedSeq[StorageObject]): Seq[User] = {
        // draw a subset of objects which is to be used in this experiment
        val objectPupularityDist = RealDistributionConfiguration.toDist(configuration.objectPopularityModel)
        val maxPop = 1.0
        val minPop = 1.0 / configuration.userCount
        val totalSumOfObjsPerUser = configuration.userCount * configuration.meanObjectCount
        val meanNumberOfUsersPerObject = configuration.userCount * { objectPupularityDist.getNumericalMean() min maxPop max minPop }
        val usedObjectCount = (totalSumOfObjsPerUser / meanNumberOfUsersPerObject).ceil.toInt

        assert(usedObjectCount <= objects.size)

        var usedObjectIndices = RandomUtils.distinctRandomSelectN(usedObjectCount, (0 until objects.size).toIndexedSeq)

        val userIndices = (0 until configuration.userCount).toIndexedSeq
        var objectSets = Map.empty[Int, Set[StorageObject]]
        usedObjectIndices foreach { objIdx =>
            val userCount = (objectPupularityDist.sample() * configuration.userCount).ceil.toInt max 1 min configuration.userCount
            val uids = RandomUtils.distinctRandomSelectN(userCount, userIndices)
            uids foreach { uid => objectSets += uid -> { objectSets.getOrElse(uid, Set.empty) + objects(objIdx) } }
        }

        // assert all users have objects
        assert(objectSets.keySet == userIndices.toSet)
        assert(objectSets.values filter { _.isEmpty } isEmpty)

        // draw the users region from a uniform distribution
        val regionDist = new UniformIntegerDistribution(1, configuration.regionCount - 1)

        val bandwidthDist = RealDistributionConfiguration.toDist(configuration.userBandwidth)

        for (i <- 0 until configuration.userCount) yield {
            val userName = "u" + (i + 1)

            val region = regionDist.sample()
            assert(region != 0)

            new User(userName, region, objectSets(i).toSeq, bandwidthDist.sample().max(64 * Units.KByte), distributor)
        }
    }
}