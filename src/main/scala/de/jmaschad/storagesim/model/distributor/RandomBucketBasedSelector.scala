package de.jmaschad.storagesim.model.distributor

import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.microcloud.CloudStatus
import de.jmaschad.storagesim.model.microcloud.AddedObject
import de.jmaschad.storagesim.model.microcloud.RequestProcessed
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.user.RequestState
import de.jmaschad.storagesim.model.user.RequestState._

class RandomBucketBasedSelector(
    val log: String => Unit,
    val send: (Int, Int, Object) => Unit) extends CloudSelector {

    var clouds = Set.empty[Int]
    var distributionGoal = Map.empty[String, Set[Int]]
    var distributionState = Map.empty[StorageObject, Set[Int]]
    var runningRequests = Set.empty[DistributorRequest]

    override def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject]) = {
        val cloudIdMap = initialClouds.map(cloud => cloud.getId -> cloud).toMap
        val bucketMap = initialObjects.groupBy(_.bucket)

        distributionGoal = createDistributionPlan(cloudIdMap.keySet, bucketMap.keySet)
        val allocationPlan = distributionGoal.foldLeft(Map.empty[Int, Set[String]])((allocMap, b) => {
            val bucket = b._1
            val clouds = b._2

            allocMap ++ clouds.map(cloud => cloud -> (allocMap.getOrElse(cloud, Set.empty) + bucket))
        })
        // we don't allocate to unknown clouds
        assert(allocationPlan.keySet.subsetOf(cloudIdMap.keySet))
        // we store exactly our buckets, with the correct replica count
        assert(allocationPlan.foldLeft(Map.empty[String, Int])((bucketCount, allocation) => {
            val cloud = allocation._1
            val buckets = allocation._2
            bucketCount ++ buckets.map(bucket => bucket -> (bucketCount.getOrElse(bucket, 0) + 1))
        }).forall(_._2 == StorageSim.configuration.replicaCount))

        allocationPlan.foreach(cloudBuckets => {
            val cloud = cloudIdMap(cloudBuckets._1)
            val buckets = cloudBuckets._2

            val cloudObjects = buckets.flatMap(bucketMap(_))
            cloud.initialize(cloudObjects)
        })

        distributionState = initialObjects.map(obj => obj -> distributionGoal(obj.bucket)).toMap
        clouds = cloudIdMap.keySet
    }

    private def createDistributionPlan(
        cloudIds: Set[Int],
        buckets: Set[String],
        currentPlan: Map[String, Set[Int]] = Map.empty): Map[String, Set[Int]] = {
        // remove unknown buckets and clouds from the current plan
        var prunedCurrentPlan = (for (bucket <- currentPlan.keys if buckets.contains(bucket)) yield {
            bucket -> currentPlan(bucket).intersect(cloudIds)
        }).toMap

        // choose clouds for buckets which have too few replicas
        prunedCurrentPlan ++= buckets.map(bucket => {
            val currentReplicas = prunedCurrentPlan.getOrElse(bucket, Set.empty)
            val requiredTargetsCount = StorageSim.configuration.replicaCount - currentReplicas.size
            requiredTargetsCount match {
                case 0 =>
                    bucket -> currentReplicas
                case n =>
                    val possibleTargets = cloudIds -- currentReplicas
                    bucket -> (currentReplicas ++ distinctRandomSelectN(requiredTargetsCount, possibleTargets.toIndexedSeq))
            }
        })

        // the new plan does not contain unknown clouds
        assert(prunedCurrentPlan.values.flatten.toSet.subsetOf(cloudIds))
        // the new plan contains exactly the given buckets
        assert(prunedCurrentPlan.keySet == buckets)
        // every bucket has the correct count of replicas
        assert(prunedCurrentPlan.values.forall(clouds => clouds.size
            == StorageSim.configuration.replicaCount))

        prunedCurrentPlan
    }

    def addCloud(cloud: Int, status: Object) = {
        throw new IllegalStateException
    }

    def removeCloud(cloud: Int) = {
        throw new IllegalStateException
    }

    override def processStatusMessage(cloud: Int, message: Object) =
        message match {
            case CloudStatus(objects) =>

            case AddedObject(storageObject) =>

            case RequestProcessed(request) =>

            case _ => throw new IllegalStateException

        }

    override def selectForPost(storageObject: StorageObject): Either[RequestState, Int] =
        Left(RequestState.UnsufficientSpace)

    override def selectForGet(storageObject: StorageObject): Either[RequestState, Int] =
        distributionState.getOrElse(storageObject, Set.empty) match {
            case targets if targets.size == 0 =>
                Left(RequestState.ObjectNotFound)
            case targets if targets.size == 1 =>
                Right(targets.head)
            case targets =>
                Right(randomSelect1(targets.toIndexedSeq))
        }

    private def randomSelect1[T](values: IndexedSeq[T]): T = distinctRandomSelectN(1, values).head

    private def distinctRandomSelectN[T](count: Int, values: IndexedSeq[_ <: T]): Set[T] = {
        assert(count > 0)
        assert(count <= values.size)

        values.size match {
            case 1 => Set(values.head)

            case n =>
                val dist = new UniformIntegerDistribution(0, n - 1)
                var distinctSample = Set.empty[Int]
                while (distinctSample.size < count) {
                    distinctSample += dist.sample()
                }
                distinctSample.map(values(_))
        }
    }
}