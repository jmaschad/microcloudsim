package de.jmaschad.storagesim.model.distributor

import scala.collection.immutable.SortedSet
import scala.math._
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.DialogEntity

class GreedyFileBasedSelector(log: String => Unit, dialogentity: DialogEntity)
    extends AbstractFileBasedSelector(log, dialogentity) {

    override protected def selectReplicationTarget(
        obj: StorageObject,
        clouds: Set[Int],
        cloudLoad: Map[Int, Double],
        preselectedClouds: Set[Int]): Int = {
        selectNextCloud(obj, cloudLoad, clouds.diff(preselectedClouds), preselectedClouds)
    }

    private def selectNextCloud(
        obj: StorageObject,
        cloudLoad: Map[Int, Double],
        availableCloudIds: Set[Int],
        preselectedCloudIds: Set[Int]): Int = {
        assert(availableCloudIds.intersect(preselectedCloudIds).isEmpty)
        val availableClouds = availableCloudIds.map(Entity.entityForId(_))
        val preselectedClouds = preselectedCloudIds.map(Entity.entityForId(_))

        val userDemand = computeUserDemand(obj)
        val cloudDemand = computeCloudDemand(preselectedClouds)
        compareClouds(
            availableClouds,
            cloudLoad,
            userDemand ++ cloudDemand,
            User.allUsers ++ preselectedClouds).head.getId
    }

    private def computeUserDemand(obj: StorageObject): Map[Entity, Double] =
        User.allUsers map { user =>
            user -> user.demand(obj) / user.medianGetDelay
        } toMap

    private def computeCloudDemand(clouds: Set[Entity]): Map[Entity, Double] =
        clouds map { _ -> 1.0 } toMap

    private def compareClouds(
        available: Set[Entity],
        load: Map[Int, Double],
        demand: Map[Entity, Double],
        requestSources: Set[Entity]): SortedSet[Entity] = {

        var cost = Map.empty[Entity, Double]
        implicit object CostOrdering extends Ordering[Entity] {
            def compare(a: Entity, b: Entity) = cost(a) compare cost(b)
        }

        cost = available map { cloud =>
            cloud -> {
                requestSources map { source =>
                    val c = NetworkDelay.between(cloud.region, source.region) * exp(pow(load(cloud.getId()), 3))
                    val d = demand(source)
                    d * c
                } sum
            }
        } toMap

        SortedSet.empty ++ available
    }
}