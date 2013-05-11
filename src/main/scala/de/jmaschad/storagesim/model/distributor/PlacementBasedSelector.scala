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

class PlacementBasedSelector(log: String => Unit, dialogCenter: DialogEntity)
    extends AbstractFileBasedSelector(log, dialogCenter) {

    private class Placement(var clouds: Set[Int], var size: Double) {
        override def toString() = "Placement(" + clouds + ", " + size + ")"
    }

    private object SizeOrdering extends Ordering[Placement] {
        def compare(x: Placement, y: Placement) = x.size == y.size match {
            case true => x.## compare y.##

            case false =>
                x.size compare y.size
        }
    }

    private var placementPool = SortedSet.empty[Placement](SizeOrdering)
    private var placements = Map.empty[StorageObject, Placement]

    override def initialize(microclouds: Set[MicroCloud], objects: Set[StorageObject], users: Set[User]) {
        val cloudCount = microclouds.size
        val cloudIDs = microclouds map { _.getId } toIndexedSeq

        val printStat = cloudCount / 10

        val allPlacements = for {
            c1 <- 0 until cloudCount
            c2 <- 0 until cloudCount if c2 != c1
            c3 <- 0 until cloudCount if c3 != c2 && c3 != c1
        } yield {
            val places = Set(cloudIDs(c1), cloudIDs(c2), cloudIDs(c3))
            new Placement(places, 0.0)
        }
        placementPool ++= allPlacements

        val tenPerc = objects.size / 10
        (1 to objects.size) zip objects foreach {
            case (idx, obj) =>
                placeObject(obj)
                if (idx % tenPerc == 0) {
                    print(".")
                }
        }
        println()

        super.initialize(microclouds, objects, users)
    }

    override def removeCloud(cloud: Int) = {
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
        val latencyOrderedTargets = possibleTargets sortWith { (t1, t2) =>
            val e1 = Entity.entityForId(t1)
            val e2 = Entity.entityForId(t2)
            NetworkDelay.between(region, e1.region) < NetworkDelay.between(region, e2.region)
        }
        latencyOrderedTargets.head
    }

    override def selectRepairSource(obj: StorageObject): Int = {
        val sources = distributionState(obj).toIndexedSeq
        val sourceLoads = sources map { ProcessingModel.loadUp(_) }

        val sample = new UniformRealDistribution(0.0, sourceLoads.sum).sample()
        var index = -1
        var selection = 0.0
        while (selection < sample) {
            index += 1
            selection += sourceLoads(index)
        }

        sources(index)
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

    private def placeObject(obj: StorageObject, possiblePlacements: SortedSet[Placement] = placementPool): Unit = {
        val placement = possiblePlacements.head
        placementPool -= placement

        placement.size += obj.size

        placementPool += placement
        placements += obj -> placement
    }

}