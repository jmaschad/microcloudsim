package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.MicroCloud
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.user.User
import scala.collection.immutable.SortedSet
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.RandomUtils
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.cloudbus.cloudsim.core.CloudSim
import scala.math._
import de.jmaschad.storagesim.Units
import de.jmaschad.storagesim.model.ProcessingModel
import org.apache.commons.math3.distribution.UniformRealDistribution
import de.jmaschad.storagesim.model.LoadPrediction
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.ProximityModel
import de.jmaschad.storagesim.model.ProcessingEntity
import org.cloudbus.cloudsim.NetworkTopology

class Placement(var clouds: Set[Int], var size: Double) {
    override def toString() = "Placement(" + clouds + ", " + size + ")"
}

class PlacementBasedSelector(log: String => Unit, dialogCenter: DialogEntity)
    extends AbstractObjectBasedSelector(log, dialogCenter) {

    private object SizeOrdering extends Ordering[Placement] {
        def compare(x: Placement, y: Placement) = x.size == y.size match {
            case true => x.## compare y.##

            case false =>
                x.size compare y.size
        }
    }

    private var placementPool = SortedSet.empty[Placement](SizeOrdering)
    private var placements = Map.empty[StorageObject, Placement]
    private var cloudLoad = Map.empty[Int, SummaryStatistics]
    private var migrations = Map.empty[StorageObject, Option[Double]]

    override def initialize(microclouds: Set[MicroCloud], objects: Set[StorageObject], users: Set[User]) {
        LoadPrediction.setUsers(users)

        val cloudCount = microclouds.size
        val cloudIDs = microclouds map { _.getId } toIndexedSeq

        val allPlacements = for {
            c1 <- 0 until cloudCount
            c2 <- 0 until cloudCount if c2 != c1
            c3 <- 0 until cloudCount if c3 != c2 && c3 != c1
        } yield {
            val places = Set(cloudIDs(c1), cloudIDs(c2), cloudIDs(c3))
            new Placement(places, 0.0)
        }
        assert(allPlacements forall { _.clouds.size == 3 })
        placementPool ++= allPlacements

        val tenPerc = objects.size / 10
        (1 to objects.size) zip objects foreach {
            case (idx, obj) =>
                placeObject(obj, placementPool)
                if (idx % tenPerc == 0) {
                    print(".")
                }
        }
        println()

        super.initialize(microclouds, objects, users)
    }

    override def removeCloud(cloud: Int) = {
        // clean the cloud load
        cloudLoad -= cloud

        // clean the placement pool
        val lostPlacements = placementPool filter { _.clouds.contains(cloud) }
        placementPool --= lostPlacements

        // find all objects which need a new placement
        val impairedObjects = placements filter {
            case (_, placement) => lostPlacements.contains(placement)
        } keySet

        // remove the lost cloud from the impaired object's placement
        impairedObjects foreach { obj =>
            placements(obj).clouds -= cloud
        }

        // find a new placement for all impaired objects
        repairObjects(impairedObjects)

        // assert no object is placed on the removed cloud
        assert({ placements.values filter { _.clouds.contains(cloud) } } isEmpty)

        super.removeCloud(cloud)
    }

    override protected def selectReplicas(obj: StorageObject, currentReplicas: Set[Int], clouds: Set[Int]): Set[Int] =
        placements(obj).clouds

    override def selectForGet(region: Int, storageObject: StorageObject): Int = {
        val possibleTargets = placements(storageObject).clouds.toIndexedSeq
        LatencyBasedSelection.selectForGet(region, possibleTargets)
    }

    override def selectRepairSource(obj: StorageObject): Int = {
        val sources = distributionState(obj).toIndexedSeq
        sources sortWith { (s1, s2) =>
            ProcessingModel.loadUp(s1).values.sum < ProcessingModel.loadUp(s2).values.sum
        } head
    }

    override def optimizePlacement(): Unit = {
        updateMigrations()

        if (!isRepairing && CloudSim.clock() > 4.0) {

            val meanLoad = ProcessingModel.allLoadUp.sum / ProcessingModel.upStats.size
            val cloudMeanLoads = ProcessingModel.upStats map {
                case (cloud, objLoad) => cloud -> objLoad.values.sum / meanLoad

            }

            val maxLoad = 1.5
            val highLoadClouds = { cloudMeanLoads filter { case (cloud, load) => load > maxLoad } keySet } map { _.getId }

            val currentMigrationSources = activeMigrations.values.flatten groupBy { _.source } keySet
            val newMigrationSources = highLoadClouds -- currentMigrationSources - StorageSim.failingCloud

            newMigrationSources foreach { migrateTopObjects(_) }
        }
    }

    private def updateMigrations(): Unit = {
        val activeMigrationObjects = { activeMigrations.values.flatten map { _.obj } toSet }
        val clock = CloudSim.clock()

        // update finished migrations
        migrations ++= migrations collect {
            case (obj, None) if !activeMigrationObjects.contains(obj) => obj -> Some(clock)
        }

        // remove old migrations
        migrations --= migrations collect {
            case (obj, Some(finishedClock)) if (clock - finishedClock) > 3.0 => obj
        }
    }

    private def migrateTopObjects(source: Int, minLoadPercentage: Double = 0.05): Unit = {
        val objLoads = ProcessingModel.loadUp(source)
        val loadToMigrate = objLoads.values.sum * minLoadPercentage

        var objectsByLoad = { objLoads filterKeys { !migrations.contains(_) } toIndexedSeq } sortWith { (l1, l2) => l1._2 > l2._2 }
        var load = 0.0
        var objectsToMigrate = Set.empty[StorageObject]
        while (load < loadToMigrate) {
            val (obj, objLoad) = objectsByLoad.head
            objectsByLoad = objectsByLoad.tail
            objectsToMigrate += obj
            load += objLoad
        }
        migrateObjects(source, objectsToMigrate)
    }

    private def migrateObjects(source: Int, objects: Set[StorageObject]): Unit = {
        val possibleMigrationTargets = objects map { obj =>
            val remainingClouds = placements(obj).clouds - source

            // remaining requests still produce a lot of load..
            if (remainingClouds.size == 3) {
                return
            }

            val ps = {
                placementPool filter { placement =>
                    val placementClouds = placement.clouds
                    (remainingClouds subsetOf placementClouds) && (!placementClouds.contains(source))
                }
            }
            val targets = { ps flatMap { _.clouds -- placements(obj).clouds } } - StorageSim.failingCloud

            obj -> targets
        } toMap

        assert(possibleMigrationTargets.values.forall { clouds => (!clouds.contains(source)) && clouds.nonEmpty })

        val meanLoad = ProcessingModel.allLoadUp.sum / ProcessingModel.upStats.size
        val loadFilteredMigrationTargets = possibleMigrationTargets map {
            case (obj, targets) =>
                obj -> {
                    targets filter { t =>
                        val targetLoad = ProcessingModel.loadUp(t).values.sum / meanLoad
                        targetLoad < 0.8
                    }
                }
        }

        assert(loadFilteredMigrationTargets.values forall { _.nonEmpty })

        loadFilteredMigrationTargets foreach {
            case (obj, targets) =>
                val sourceNetID = Entity.entityForId(source).netID
                val target = targets minBy { t => NetworkTopology.getDelay(sourceNetID, Entity.entityForId(t).netID) }

                val placement = placements(obj)
                placementPool -= placement

                placement.clouds -= source
                placement.clouds += target
                assert(placement.clouds.size == StorageSim.configuration.replicaCount)

                placementPool += placement
                migrate(obj, source, target)
                migrations += obj -> None
        }
        assert(placements forall {
            case (obj, placement) =>
                val correctCount = placement.clouds.size == StorageSim.configuration.replicaCount
                if (!correctCount) {
                    println("Problem: " + obj)
                    println()
                }
                correctCount
        })
    }

    private def repairObjects(objects: Set[StorageObject]) = {
        val neighborPlacements = objects map { obj =>
            obj -> {
                placementPool filter { placement =>
                    val currentClouds = placements(obj).clouds
                    val placementClouds = placement.clouds
                    (currentClouds subsetOf placementClouds) && (currentClouds != placementClouds)
                }
            }
        } toMap

        var cloudDownAmount = Map.empty[Int, Double]
        object DownAmountOrdering extends Ordering[Placement] {
            def compare(x: Placement, y: Placement) = {
                val maxDownAmountX = x.clouds map { cloudDownAmount.getOrElse(_, 0.0) } max
                val maxDownAmountY = y.clouds map { cloudDownAmount.getOrElse(_, 0.0) } max

                val dist = abs(maxDownAmountX - maxDownAmountY)
                if (dist < 10 * Units.MByte)
                    SizeOrdering.compare(x, y)
                else
                    maxDownAmountX compare maxDownAmountY
            }
        }

        objects foreach { obj =>
            val currentClouds = placements(obj).clouds
            placeObject(obj, SortedSet.empty(DownAmountOrdering) ++ neighborPlacements(obj))
            val addedClouds = placements(obj).clouds -- currentClouds
            addedClouds foreach { c => cloudDownAmount += c -> { cloudDownAmount.getOrElse(c, 0.0) + obj.size } }
        }
    }

    private def placeObject(obj: StorageObject, availablePlacements: SortedSet[Placement]): Unit = {
        val placement = if (LoadPrediction.getLoad(obj) > 0.0) {
            // select the lowest proximity placement out of a fraction of the remaining placements
            val topFractionPercentage = 0.3
            val topFractionSize = (availablePlacements.size * topFractionPercentage).ceil.toInt
            if (topFractionSize > 1) {
                val consideredPlacements = availablePlacements take topFractionSize
                ProximityModel.selectLowestDistance(consideredPlacements, LoadPrediction.getUserLoads(obj))
            } else {
                availablePlacements.head
            }
        } else {
            availablePlacements.head
        }

        placementPool -= placement

        placement.size += obj.size

        placementPool += placement
        placements += obj -> placement

        placement.clouds foreach { cloud =>
            if (!cloudLoad.isDefinedAt(cloud)) {
                cloudLoad += cloud -> new SummaryStatistics()
            }
            cloudLoad(cloud).addValue(LoadPrediction.getLoad(obj))
        }
    }

    private def filterByLoad(maxLoad: Double, availablePlacements: SortedSet[Placement]): SortedSet[Placement] = {
        val overAllMean = new DescriptiveStatistics(cloudLoad.values map { _.getMean() } toArray).getMean

        if (overAllMean > 0.0) {
            availablePlacements filter { p =>
                p.clouds forall { c =>
                    if (cloudLoad.isDefinedAt(c)) {
                        val load = (cloudLoad(c).getMean / overAllMean)
                        load <= maxLoad
                    } else {
                        true
                    }
                }
            } ensuring { _.nonEmpty }
        } else {
            availablePlacements
        }
    }
}
