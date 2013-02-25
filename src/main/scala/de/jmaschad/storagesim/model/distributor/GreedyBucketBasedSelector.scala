package de.jmaschad.storagesim.model.distributor

import scala.collection.immutable.SortedSet
import scala.math._

import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.user.User

class GreedyBucketBasedSelector(log: String => Unit, dialogCenter: DialogCenter)
    extends AbstractBucketBasedSelector(log, dialogCenter) {

    override protected def selectReplicationTarget(
        bucket: String,
        clouds: Set[Int],
        cloudLoad: Map[Int, Double],
        preselectedClouds: Set[Int]): Int = {
        selectNextCloud(bucket, cloudLoad, clouds.diff(preselectedClouds), preselectedClouds)
    }

    private def selectNextCloud(
        bucket: String,
        cloudLoad: Map[Int, Double],
        availableCloudIds: Set[Int],
        preselectedCloudIds: Set[Int]): Int = {
        assert(availableCloudIds.intersect(preselectedCloudIds).isEmpty)

        val availableClouds = availableCloudIds.map(Entity.entityForId(_))
        val preselectedClouds = preselectedCloudIds.map(Entity.entityForId(_))

        val userDemand = computeUserDemand(bucket)
        val cloudDemand = computeCloudDemand(preselectedClouds)

        compareClouds(
            availableClouds,
            cloudLoad,
            userDemand ++ cloudDemand,
            User.allUsers ++ preselectedClouds).head.getId
    }

    private def computeUserDemand(bucket: String): Map[Entity, Double] =
        User.allUsers.map(user => {
            user -> user.objects.collect({ case obj if obj.bucket == bucket => user.demand(obj) }).sum
        }).toMap

    private def computeCloudDemand(clouds: Set[Entity]): Map[Entity, Double] =
        clouds.map(_ -> 1.0).toMap

    private def compareClouds(
        available: Set[Entity],
        load: Map[Int, Double],
        demand: Map[Entity, Double],
        requestSources: Set[Entity]): SortedSet[Entity] = {

        var cost = Map.empty[Entity, Double]
        implicit object CostOrdering extends Ordering[Entity] {
            def compare(a: Entity, b: Entity) = cost(a).compare(cost(b))
        }

        cost = available.map(cloud => {
            cloud -> requestSources.map(source => {
                val c = NetworkDelay.between(cloud.region, source.region) * exp(pow(load(cloud.getId()), 3))
                val d = demand(source)
                d * c
            }).sum
        }).toMap

        SortedSet.empty ++ available
    }
}