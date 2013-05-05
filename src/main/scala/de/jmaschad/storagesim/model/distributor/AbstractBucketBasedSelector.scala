package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.user.User

abstract class AbstractBucketBasedSelector(
    val log: String => Unit,
    val dialogEntity: DialogEntity) extends CloudSelector {

    // currently online clouds
    private var clouds = Set.empty[Int]

    // goal bucket cloud distribution
    private var distributionGoal = Map.empty[String, Set[Int]]

    // current object cloud distribution
    private var distributionState = Map.empty[StorageObject, Set[Int]]

    // current replication actions
    private case class ReplicationAction(source: Int, target: Int, obj: StorageObject)
    private var activeReplications = Set.empty[ReplicationAction]

    override def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject], users: Set[User]) = {
        val cloudIdMap = initialClouds map { cloud => cloud.getId -> cloud } toMap
        val bucketMap = initialObjects groupBy { _.bucket }

        distributionGoal = createDistributionPlan(cloudIdMap.keySet, bucketMap)
        val allocationPlan = distributionGoal.foldLeft(Map.empty[Int, Set[String]]) {
            case (allocation, (bucket, clouds)) =>
                allocation ++ (clouds map { cloud =>
                    cloud -> (allocation.getOrElse(cloud, Set.empty) + bucket)
                })
        }

        // we don't allocate to unknown clouds
        assert(allocationPlan.keySet.subsetOf(cloudIdMap.keySet))

        // we store exactly our buckets, with the correct replica count
        assert(
            allocationPlan.foldLeft(Map.empty[String, Int]) { (bucketCount, allocation) =>
                val cloud = allocation._1
                val buckets = allocation._2
                bucketCount ++ buckets.map(bucket => bucket -> (bucketCount.getOrElse(bucket, 0) + 1))
            } forall { _._2 == StorageSim.configuration.replicaCount })

        allocationPlan foreach {
            case (cloudId, buckets) =>
                val cloudObjects = buckets flatMap { bucketMap(_) }
                cloudIdMap(cloudId).initialize(cloudObjects)
        }

        distributionState = {
            initialObjects map { obj => obj -> distributionGoal(obj.bucket) }
        } toMap

        clouds = cloudIdMap.keySet
    }

    override def selectForPost(storageObject: StorageObject): Either[RequestSummary, Int] =
        Left(UnsufficientSpace)

    override def selectForGet(region: Int, storageObject: StorageObject): Either[RequestSummary, Int] =
        distributionState getOrElse (storageObject, Set.empty) match {
            case targets if targets.size == 0 =>
                Left(ObjectNotFound)
            case targets if targets.size == 1 =>
                Right(targets.head)
            case targets =>
                var minDelay = Double.MaxValue
                var minDelayTarget = targets.head
                for (target <- targets) {
                    val targetEntity = Entity.entityForId(target)
                    val delay = NetworkDelay.between(region, targetEntity.region)
                    if (delay < minDelay) {
                        minDelay = delay
                        minDelayTarget = target
                    }
                }
                Right(minDelayTarget)
        }

    def addedObject(cloud: Int, obj: StorageObject): Unit = {}

    def addCloud(cloud: Int) =
        clouds += cloud

    def removeCloud(cloud: Int) = {
        // update knowledge of current state
        assert(clouds.contains(cloud))
        clouds -= cloud
        distributionState = distributionState mapValues { _ - cloud }

        // throw if objects were lost
        val lostObjects = distributionState filter { case (_, clouds) => clouds.isEmpty }
        if (lostObjects.nonEmpty) {
            throw new IllegalStateException("all copies of " + lostObjects.mkString(", ") + " were lost")
        }

        // create new distribution goal
        val bucketObjectMap = distributionState.keySet groupBy { _.bucket }
        distributionGoal = createDistributionPlan(clouds, bucketObjectMap, distributionGoal)

        // create an action plan and inform the involved clouds
        val repairPlan = createRepairPlan(distributionGoal, distributionState, activeReplications)
        repairPlan foreach { case (cloud, load) => sendActions(cloud, load) }

        // update the active replications
        activeReplications ++= repairPlan flatMap {
            case (cloud, load) =>
                load.objSourceMap map {
                    case (obj, source) =>
                        ReplicationAction(source, cloud, obj)
                }
        }
    }

    protected def selectReplicationTarget(
        bucket: String,
        clouds: Set[Int],
        cloudLoad: Map[Int, Double],
        preselectedClouds: Set[Int]): Int

    private def createDistributionPlan(
        cloudIds: Set[Int],
        bucketMap: Map[String, Set[StorageObject]],
        currentPlan: Map[String, Set[Int]] = Map.empty): Map[String, Set[Int]] = {

        val buckets = bucketMap.keySet

        // remove unknown buckets and clouds from the current plan
        var distributionPlan = currentPlan.keys collect {
            case bucket if buckets.contains(bucket) =>
                bucket -> currentPlan(bucket).intersect(cloudIds)
        } toMap

        // choose clouds for buckets which have too few replicas
        distributionPlan ++= buckets map { bucket =>
            val currentReplicas = distributionPlan.getOrElse(bucket, Set.empty)
            val requiredTargetsCount = StorageSim.configuration.replicaCount - currentReplicas.size
            requiredTargetsCount match {
                case 0 =>
                    bucket -> currentReplicas
                case n =>
                    bucket -> selectReplicas(n, bucket, cloudIds, bucketMap, distributionPlan)
            }
        }

        // the new plan does not contain unknown clouds
        assert(distributionPlan.values.flatten.toSet.subsetOf(cloudIds))
        // the new plan contains exactly the given buckets
        assert(distributionPlan.keySet == buckets)
        // every bucket has the correct count of replicas
        assert(distributionPlan.values forall { clouds =>
            clouds.size == StorageSim.configuration.replicaCount
        })

        distributionPlan
    }

    private def selectReplicas(
        count: Int,
        bucket: String,
        clouds: Set[Int],
        bucketMap: Map[String, Set[StorageObject]],
        distributionPlan: Map[String, Set[Int]]): Set[Int] =
        count match {
            case 0 =>
                distributionPlan.getOrElse(bucket, Set.empty)
            case n =>
                val load = cloudLoad(clouds, distributionPlan, bucketMap)
                val currentReplicas = distributionPlan.getOrElse(bucket, Set.empty)
                val selection = selectReplicationTarget(bucket, clouds, load, currentReplicas)
                val newDistributionPlan = distributionPlan + (bucket -> (distributionPlan.getOrElse(bucket, Set.empty) + selection))
                selectReplicas(count - 1, bucket, clouds, bucketMap, newDistributionPlan)
        }

    private def cloudLoad(clouds: Set[Int], distributionPlan: Map[String, Set[Int]], bucketMap: Map[String, Set[StorageObject]]): Map[Int, Double] = {
        val sizeDistribution = distributionPlan.foldLeft(Map.empty[Int, Double]) {
            case (plan, (bucket, clouds)) =>
                val bucketSize = bucketMap(bucket) map { _.size } sum
                val sizeDist = clouds.map {
                    _ -> bucketSize
                } groupBy {
                    case (cloud, _) => cloud
                } mapValues { cloudSizeSet =>
                    cloudSizeSet map { case (_, size) => size } sum
                }

                plan ++ sizeDist
        }

        // normalize
        if (sizeDistribution.nonEmpty) {
            val max = sizeDistribution.values.max
            sizeDistribution mapValues { _ / max }
        }

        clouds map { c => c -> sizeDistribution.getOrElse(c, 0.0) } toMap
    }

    private def createRepairPlan(
        distributionGoal: Map[String, Set[Int]],
        distributionState: Map[StorageObject, Set[Int]],
        activeReplications: Set[ReplicationAction]): Map[Int, Load] = {

        val additionalCloudMap = distributionState map {
            case (obj, currentClouds) =>
                val additionalClouds = distributionGoal(obj.bucket) diff currentClouds
                obj -> additionalClouds
        }

        val additionalObjectMap = additionalCloudMap.toSeq flatMap { objClouds =>
            objClouds._2.map(objClouds._1 -> _)
        } groupBy {
            case (_, cloud) => cloud
        } mapValues { objCloud =>
            objCloud map { case (obj, _) => obj } toSet
        }

        additionalObjectMap.mapValues { objects =>
            Load(objects map { obj =>
                obj -> RandomUtils.randomSelect1(distributionState(obj).toIndexedSeq)
            } toMap)
        }
    }

    private def sendActions(cloud: Int, request: PlacementDialog): Unit = {
        val dialog = dialogEntity.openDialog(cloud)
        dialog.messageHandler = (content) => content match {
            case PlacementAck => dialog.close()
        }

        dialog.say(request, () => { throw new IllegalStateException })
    }

}