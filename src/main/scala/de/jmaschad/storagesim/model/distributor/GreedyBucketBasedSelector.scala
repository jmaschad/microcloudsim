package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.Entity
import scala.collection.immutable.SortedSet
import de.jmaschad.storagesim.model.NetworkDelay

class GreedyBucketBasedSelector(log: String => Unit, dialogCenter: DialogCenter)
    extends AbstractBucketBasedSelector(log, dialogCenter) {

    protected def selectReplicationTargets(bucket: String, count: Int, clouds: Set[Int], preselectedClouds: Set[Int]): Set[Int] =
        count match {
            case 0 =>
                preselectedClouds

            case n if n > 0 =>
                val cloud = selectNextCloud(bucket, clouds.diff(preselectedClouds), preselectedClouds)
                selectReplicationTargets(bucket, count - 1, clouds, preselectedClouds + cloud)

            case _ =>
                throw new IllegalStateException
        }

    private def selectNextCloud(bucket: String, availableCloudIds: Set[Int], preselectedCloudIds: Set[Int]): Int = {
        assert(availableCloudIds.intersect(preselectedCloudIds).isEmpty)
        val availableClouds = availableCloudIds.map(Entity.entityForId(_))
        val preselectedClouds = preselectedCloudIds.map(Entity.entityForId(_))

        val userDemand = computeUserDemand(bucket)
        val cloudDemand = computeCloudDemand(preselectedClouds)
        compareClouds(
            availableClouds,
            userDemand ++ cloudDemand,
            User.allUsers ++ preselectedClouds).head.getId
    }

    private def computeUserDemand(bucket: String): Map[Entity, Double] =
        User.allUsers.map(user => {
            user -> user.objects.collect({ case obj if obj.bucket == bucket => user.demand(obj) }).sum
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