package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.user.User

abstract class AbstractFileBasedSelector(
    log: String => Unit,
    dialogEntity: DialogEntity) extends CloudSelector {
    private var distributionState = Map.empty[StorageObject, Set[Int]]
    private var distributionGoal = Map.empty[StorageObject, Set[Int]]
    private var clouds = Set.empty[Int]

    private var activeOperations = Set.empty[RepairTracker]
    private var activeDownloads = Set.empty[DownloadRequest]

    override def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject], users: Set[User]): Unit = {
        val cloudIdMap = { initialClouds map { cloud => cloud.getId -> cloud } toMap }
        distributionGoal = createDistributionPlan(cloudIdMap.keySet, initialObjects)

        val allocationPlan = distributionGoal.foldLeft(Map.empty[Int, Set[StorageObject]]) {
            case (allocation, (obj, clouds)) =>
                allocation ++ clouds.map(cloud => cloud -> (allocation.getOrElse(cloud, Set.empty) + obj))
        }

        // we don't allocate to unknown clouds
        assert(allocationPlan.keySet.subsetOf(cloudIdMap.keySet))
        // we store exactly our objects, with the correct replica count
        assert(allocationPlan.foldLeft(Map.empty[StorageObject, Int]) {
            case (bucketCount, (cloud, objects)) =>
                bucketCount ++ objects.map(bucket => bucket -> (bucketCount.getOrElse(bucket, 0) + 1))
        } forall {
            _._2 == StorageSim.configuration.replicaCount
        })

        allocationPlan foreach { case (cloudId, objects) => cloudIdMap(cloudId).initialize(objects) }

        distributionState = distributionGoal
        clouds = cloudIdMap.keySet
    }

    override def addCloud(cloud: Int): Unit =
        clouds += cloud

    override def removeCloud(cloud: Int): Unit = {
        // update knowledge of current state
        assert(clouds.contains(cloud))
        clouds -= cloud
        distributionState = distributionState mapValues { _ - cloud }

        activeDownloads = activeDownloads filterNot { _.cloud == cloud }
        activeOperations map { _.removeCloud(cloud) }

        // throw if objects were lost
        val lostObjects = distributionState filter { _._2.isEmpty }
        if (lostObjects.nonEmpty) {
            throw new IllegalStateException("all copies of " + lostObjects.mkString(", ") + " were lost")
        }

        // create new distribution goal
        distributionGoal = createDistributionPlan(clouds, distributionState.keySet, distributionGoal)

        // create an action plan and inform the involved clouds
        val actionPlan = createActionPlan(distributionGoal, distributionState, activeDownloads)
        val newDownloads = {
            actionPlan flatMap {
                case (cloudId, action) => action.objSourceMap.keys.map(DownloadRequest(cloudId, _))
            } toSet
        }
        activeOperations += new RepairTracker(log, newDownloads)

        actionPlan foreach { case (cloudId, action) => sendActions(cloudId, action) }
    }

    def startedDownload(cloud: Int, obj: StorageObject): Unit = {
        val download = DownloadRequest(cloud, obj)
        assert(!activeDownloads.contains(download))
        activeDownloads += download
    }

    def finishedDownload(cloud: Int, obj: StorageObject): Unit = {
        val download = DownloadRequest(cloud, obj)
        assert(activeDownloads.contains(download))
        activeDownloads -= download

        activeOperations map { _.complete(download) }
        activeOperations = activeOperations filterNot { _.isDone }
    }

    override def selectForPost(storageObject: StorageObject): Either[RequestSummary, Int] =
        Left(UnsufficientSpace)

    override def selectForGet(region: Int, storageObject: StorageObject): Either[RequestSummary, Int] =
        distributionState.getOrElse(storageObject, Set.empty) match {
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

    protected def selectReplicationTarget(obj: StorageObject, clouds: Set[Int], cloudLoad: Map[Int, Double], preselectedClouds: Set[Int]): Int

    private def createDistributionPlan(
        cloudIds: Set[Int],
        objects: Set[StorageObject],
        currentPlan: Map[StorageObject, Set[Int]] = Map.empty): Map[StorageObject, Set[Int]] = {

        // remove unknown objects and clouds from the current plan
        var distributionPlan = currentPlan.keys collect { case obj if objects.contains(obj) => obj -> currentPlan(obj).intersect(cloudIds) } toMap

        // choose clouds for buckets which have too few replicas
        distributionPlan ++= objects map { obj =>
            val currentReplicas = distributionPlan.getOrElse(obj, Set.empty)
            val requiredTargetsCount = StorageSim.configuration.replicaCount - currentReplicas.size
            requiredTargetsCount match {
                case 0 =>
                    obj -> currentReplicas
                case n =>
                    obj -> selectReplicas(n, obj, cloudIds, distributionPlan)
            }
        }

        // the new plan does not contain unknown clouds
        assert(distributionPlan.values.flatten.toSet.subsetOf(cloudIds))
        // the new plan contains exactly the given buckets
        assert(distributionPlan.keySet == objects)
        // every bucket has the correct count of replicas
        assert(distributionPlan.values forall { clouds =>
            clouds.size == StorageSim.configuration.replicaCount
        })

        distributionPlan
    }

    private def selectReplicas(
        count: Int,
        obj: StorageObject,
        clouds: Set[Int],
        distributionPlan: Map[StorageObject, Set[Int]]): Set[Int] =
        count match {
            case 0 =>
                distributionPlan.getOrElse(obj, Set.empty)
            case n =>
                val load = cloudLoad(clouds, distributionPlan)
                val currentReplicas = distributionPlan.getOrElse(obj, Set.empty)
                val selection = selectReplicationTarget(obj, clouds, load, currentReplicas)
                val newDistributionPlan = distributionPlan + (obj -> (distributionPlan.getOrElse(obj, Set.empty) + selection))
                selectReplicas(count - 1, obj, clouds, newDistributionPlan)
        }

    private def cloudLoad(clouds: Set[Int], distributionPlan: Map[StorageObject, Set[Int]]): Map[Int, Double] = {
        val sizeDistribution = distributionPlan.foldLeft(Map.empty[Int, Double])((plan, b) => {
            val obj = b._1
            val clouds = b._2
            val sizeDist = clouds.map(c => c -> obj.size).groupBy(_._1).mapValues(_.map(_._2).sum)
            plan ++ sizeDist
        })

        // normalize
        if (sizeDistribution.nonEmpty) {
            val max = sizeDistribution.values.max
            sizeDistribution mapValues { _ / max }
        }

        clouds.map(c => c -> sizeDistribution.getOrElse(c, 0.0)).toMap
    }

    private def createActionPlan(
        distributionGoal: Map[StorageObject, Set[Int]],
        distributionState: Map[StorageObject, Set[Int]],
        activeTransactions: Set[DownloadRequest]): Map[Int, Load] = {

        val additionalCloudMap = distributionState map { objectCloudMap =>
            val storageObject = objectCloudMap._1
            val currentClouds = objectCloudMap._2
            val additionalClouds = distributionGoal(storageObject) diff currentClouds
            storageObject -> additionalClouds
        }

        val additionalObjectMap = additionalCloudMap.toSeq flatMap {
            case (obj, cloudIds) =>
                cloudIds map { obj -> _ }
        } groupBy { _._2 } mapValues { _ map { _._1 } toSet }

        additionalObjectMap mapValues { objects =>
            Load({
                objects map { obj =>
                    obj -> RandomUtils.randomSelect1(distributionState(obj).toIndexedSeq)
                } toMap
            })
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