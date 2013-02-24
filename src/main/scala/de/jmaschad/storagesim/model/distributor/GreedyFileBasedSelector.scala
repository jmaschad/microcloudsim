package de.jmaschad.storagesim.model.distributor

import scala.collection.immutable.Set
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.processing.StorageObject
import scala.collection.immutable.SortedSet
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.NetworkDelay

class GreedyFileBasedSelector(log: String => Unit, dialogCenter: DialogCenter)
    extends AbstractFileBasedSelector(log, dialogCenter) {

    override protected def selectReplicationTargets(
        obj: StorageObject,
        count: Int,
        clouds: Set[Int],
        preselectedClouds: Set[Int]): Set[Int] = count match {
        case 0 =>
            preselectedClouds

        case n if n > 0 =>
            val cloud = selectNextCloud(obj, clouds.diff(preselectedClouds), preselectedClouds)
            selectReplicationTargets(obj, count - 1, clouds, preselectedClouds + cloud)

        case _ =>
            throw new IllegalStateException
    }

    private def selectNextCloud(obj: StorageObject, availableCloudIds: Set[Int], preselectedCloudIds: Set[Int]): Int = {
        assert(availableCloudIds.intersect(preselectedCloudIds).isEmpty)
        val availableClouds = availableCloudIds.map(Entity.entityForId(_))
        val preselectedClouds = preselectedCloudIds.map(Entity.entityForId(_))

        val userDemand = computeUserDemand(obj)
        val cloudDemand = computeCloudDemand(preselectedClouds)
        compareClouds(
            availableClouds,
            userDemand ++ cloudDemand,
            User.allUsers ++ preselectedClouds).head.getId
    }

    private def computeUserDemand(obj: StorageObject): Map[Entity, Double] =
        User.allUsers.map(user => {
            user -> user.demand(obj)
        }).toMap

    private def computeCloudDemand(clouds: Set[Entity]): Map[Entity, Double] =
        clouds.map(_ -> 1.0).toMap

    private def compareClouds(available: Set[Entity], demand: Map[Entity, Double], requestSources: Set[Entity]): SortedSet[Entity] = {
        var cost = Map.empty[Entity, Double]
        implicit object CostOrdering extends Ordering[Entity] {
            def compare(a: Entity, b: Entity) = cost(a).compare(cost(b))
        }

        cost = available.map(entity => {
            entity -> requestSources.map(source => demand(source) * NetworkDelay.between(entity.region, source.region)).sum
        }).toMap

        SortedSet.empty ++ available
    }
}