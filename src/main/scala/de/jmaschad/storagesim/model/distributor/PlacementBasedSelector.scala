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

class PlacementBasedSelector(log: String => Unit, dialogCenter: DialogEntity)
    extends AbstractFileBasedSelector(log, dialogCenter) {

    private class Placement(val clouds: Set[Int], var size: Double) {
        override def toString() = "Placement(" + clouds + ", " + size + ")"
    }

    private object SizeOrdering extends Ordering[Placement] {
        def compare(x: Placement, y: Placement) = x.size == y.size match {
            case true => x.## compare y.##
            case false => x.size compare y.size
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
            val size = 0.0
            new Placement(places, size)
        }
        placementPool ++= allPlacements

        objects foreach { placeObject(_) }

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

        // find a new placement for all impaired objects
        impairedObjects foreach { obj =>
            val remainingClouds = placements(obj).clouds - cloud
            replaceObject(obj, remainingClouds)
        }

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
        possibleTargets.toIndexedSeq(new UniformIntegerDistribution(0, possibleTargets.size - 1).sample())
    }

    override def selectRepairSource(obj: StorageObject): Int =
        RandomUtils.randomSelect1(distributionState(obj).toIndexedSeq)

    private def replaceObject(obj: StorageObject, currentClouds: Set[Int]) = {
        val neighborPlacements = placementPool filter { currentClouds subsetOf _.clouds }
        placeObject(obj, neighborPlacements)
    }

    private def placeObject(obj: StorageObject, possiblePlacements: SortedSet[Placement] = placementPool): Unit = {
        val placement = possiblePlacements.head
        placementPool -= placement

        placement.size += obj.size

        placementPool += placement
        placements += obj -> placement
    }

}