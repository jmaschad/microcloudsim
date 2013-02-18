package de.jmaschad.storagesim.model.distributor

import scala.collection.mutable
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.RequestType
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.CloudStatus
import de.jmaschad.storagesim.model.user.RequestState
import de.jmaschad.storagesim.model.user.RequestState._
import de.jmaschad.storagesim.model.microcloud.CloudStatus
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.AddedObject
import de.jmaschad.storagesim.model.microcloud.RequestProcessed
import de.jmaschad.storagesim.model.microcloud.StatusMessage
import de.jmaschad.storagesim.model.microcloud.InterCloudRequest
import de.jmaschad.storagesim.model.microcloud.Load
import de.jmaschad.storagesim.model.microcloud.InterCloudRequest
import de.jmaschad.storagesim.model.microcloud.CancelRequest
import de.jmaschad.storagesim.model.microcloud.Remove

class RandomBucketBasedSelector(
    val log: String => Unit,
    val send: (Int, Int, Object) => Unit) extends CloudSelector {
    var clouds = Set.empty[Int]
    var distributionGoal = Map.empty[String, Set[Int]]
    var distributionState = Map.empty[StorageObject, Set[Int]]
    var runningRequests = Set.empty[InterCloudRequest]

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

    override def addCloud(cloud: Int, status: Object) =
        clouds += cloud

    override def removeCloud(cloud: Int) = {
        clouds -= cloud
        distributionState = distributionState.mapValues(_ - cloud)
        purgeRequests(cloud)

        distributionGoal = createDistributionPlan(clouds, distributionGoal.keySet, distributionGoal)
        val adaptionPlan = createAdaptionPlan(distributionGoal, distributionState)

        val obsoleteRequests = runningRequests -- adaptionPlan
        cancelRequests(obsoleteRequests)

        val addedRequests = adaptionPlan -- runningRequests
        sendRequests(addedRequests)

        runningRequests ++= addedRequests
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

    private def createAdaptionPlan(
        distributionGoal: Map[String, Set[Int]],
        distributionState: Map[StorageObject, Set[Int]]): Set[InterCloudRequest] =
        distributionState.foldLeft(Set.empty[InterCloudRequest])((requestSet, objectCloudMap) => {
            val storageObject = objectCloudMap._1
            val currentClouds = objectCloudMap._2
            val additionalClouds = distributionGoal(storageObject.bucket) diff currentClouds
            requestSet ++ additionalClouds.map(Load(randomSelect1(currentClouds.toIndexedSeq), _, storageObject))
        })

    private def purgeRequests(cloud: Int) = {
        runningRequests = runningRequests.filterNot(req => req match {
            case Load(_, target, _) => target == cloud

            case _ => throw new IllegalStateException
        })

    }

    private def cancelRequests(requests: Set[InterCloudRequest]) = requests.foreach(req => req match {
        case req: InterCloudRequest =>
            send(req.requestHandler, MicroCloud.InterCloudRequest, CancelRequest(req))

        case _ => throw new IllegalStateException
    })

    private def sendRequests(requests: Set[InterCloudRequest]) = requests.foreach(req => req match {
        case load @ Load(_, target, _) =>
            send(target, MicroCloud.InterCloudRequest, load)

        case _ => throw new IllegalStateException
    })

    override def processStatusMessage(cloud: Int, message: Object) =
        message match {
            case CloudStatus(objects) =>

            case AddedObject(storageObject) =>
                addObject(cloud, storageObject)

            case RequestProcessed(request) =>
                assert(runningRequests.contains(request), "unknown request " + request)
                runningRequests -= request

            case _ => throw new IllegalStateException

        }

    private def addObject(cloud: Int, storageObject: StorageObject) =
        if (distributionGoal.isDefinedAt(storageObject.bucket) && distributionGoal(storageObject.bucket).contains(cloud)) {
            distributionState += storageObject -> (distributionState.getOrElse(storageObject, Set.empty) + cloud)
        } else {
            send(cloud, MicroCloud.InterCloudRequest, Remove(cloud, storageObject))
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