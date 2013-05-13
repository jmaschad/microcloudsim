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

class Placement(var clouds: Set[Int], var size: Double) {
    override def toString() = "Placement(" + clouds + ", " + size + ")"
}

class PlacementBasedSelector(log: String => Unit, dialogCenter: DialogEntity)
    extends AbstractFileBasedSelector(log, dialogCenter) {

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
            ProcessingModel.loadUp(s1) < ProcessingModel.loadUp(s2)
        } head
    }

    override def optimizePlacement(): Unit = if (!isRepairing) {
        // TODO change the placements of objects to migrate and call corresponding startMigrations
    }

    private def repairObjects(objects: Set[StorageObject]) = {
        val currentClouds = objects map { obj => obj -> placements(obj).clouds } toMap
        val neighborPlacements = objects map { obj =>
            obj -> { placementPool filter { placement => currentClouds(obj) subsetOf placement.clouds } }
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
            placeObject(obj, SortedSet.empty(DownAmountOrdering) ++ neighborPlacements(obj))
            val addedClouds = placements(obj).clouds -- currentClouds(obj)
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
