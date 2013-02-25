package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageObject

abstract class AbstractFileBasedSelector(
    log: String => Unit,
    dialogCenter: DialogCenter) extends CloudSelector {
    private var distributionState = Map.empty[StorageObject, Set[Int]]
    private var distributionGoal = Map.empty[StorageObject, Set[Int]]
    private var clouds = Set.empty[Int]

    private var activeOperations = Set.empty[RepairTracker]
    private var activeDownloads = Set.empty[DownloadRequest]

    override def initialize(initialClouds: Set[MicroCloud], initialObjects: Set[StorageObject]): Unit = {
        val cloudIdMap = initialClouds.map(cloud => cloud.getId -> cloud).toMap
        distributionGoal = createDistributionPlan(cloudIdMap.keySet, initialObjects)

        val allocationPlan = distributionGoal.foldLeft(Map.empty[Int, Set[StorageObject]])((allocMap, b) => {
            val obj = b._1
            val clouds = b._2

            allocMap ++ clouds.map(cloud => cloud -> (allocMap.getOrElse(cloud, Set.empty) + obj))
        })

        // we don't allocate to unknown clouds
        assert(allocationPlan.keySet.subsetOf(cloudIdMap.keySet))
        // we store exactly our objects, with the correct replica count
        assert(allocationPlan.foldLeft(Map.empty[StorageObject, Int])((bucketCount, allocation) => {
            val cloud = allocation._1
            val objects = allocation._2
            bucketCount ++ objects.map(bucket => bucket -> (bucketCount.getOrElse(bucket, 0) + 1))
        }).forall(_._2 == StorageSim.configuration.replicaCount))

        allocationPlan.foreach(cloudObjects => {
            val cloud = cloudIdMap(cloudObjects._1)
            val objects = cloudObjects._2
            cloud.initialize(objects)
        })

        distributionState = distributionGoal
        clouds = cloudIdMap.keySet
    }

    override def addCloud(cloud: Int): Unit =
        clouds += cloud

    override def removeCloud(cloud: Int): Unit = {
        // update knowledge of current state
        assert(clouds.contains(cloud))
        clouds -= cloud
        distributionState = distributionState.mapValues(_ - cloud)

        activeDownloads = activeDownloads.filterNot(_.cloud == cloud)
        activeOperations.map(_.removeCloud(cloud))

        // throw if objects were lost
        val lostObjects = distributionState.filter(_._2.isEmpty)
        if (lostObjects.nonEmpty) {
            throw new IllegalStateException("all copies of " + lostObjects.mkString(", ") + " were lost")
        }

        // create new distribution goal
        distributionGoal = createDistributionPlan(clouds, distributionState.keySet, distributionGoal)

        // create an action plan and inform the involved clouds
        val actionPlan = createActionPlan(distributionGoal, distributionState, activeDownloads)
        val newDownloads = actionPlan.flatMap(m => {
            m._2.objSourceMap.keys.map(DownloadRequest(m._1, _))
        }).toSet
        activeOperations += new RepairTracker(log, newDownloads)

        actionPlan.foreach(cloudPlan => sendActions(cloudPlan._1, cloudPlan._2))
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

        activeOperations.map(_.complete(download))
        activeOperations = activeOperations.filterNot(_.isDone)
    }

    override def selectForPost(storageObject: StorageObject): Either[RequestSummary, Int] =
        Left(UnsufficientSpace)

    override def selectForGet(storageObject: StorageObject): Either[RequestSummary, Int] =
        distributionState.getOrElse(storageObject, Set.empty) match {
            case targets if targets.size == 0 =>
                Left(ObjectNotFound)
            case targets if targets.size == 1 =>
                Right(targets.head)
            case targets =>
                Right(RandomUtils.randomSelect1(targets.toIndexedSeq))
        }

    protected def selectReplicationTarget(obj: StorageObject, clouds: Set[Int], cloudLoad: Map[Int, Double], preselectedClouds: Set[Int]): Int

    private def createDistributionPlan(
        cloudIds: Set[Int],
        objects: Set[StorageObject],
        currentPlan: Map[StorageObject, Set[Int]] = Map.empty): Map[StorageObject, Set[Int]] = {

        // remove unknown objects and clouds from the current plan
        var distributionPlan = (for (obj <- currentPlan.keys if objects.contains(obj)) yield {
            obj -> currentPlan(obj).intersect(cloudIds)
        }).toMap

        // choose clouds for buckets which have too few replicas
        distributionPlan ++= objects.map(obj => {
            val currentReplicas = distributionPlan.getOrElse(obj, Set.empty)
            val requiredTargetsCount = StorageSim.configuration.replicaCount - currentReplicas.size
            log("selecting replicas for object " + obj + "/" + objects.size)
            requiredTargetsCount match {
                case 0 =>
                    obj -> currentReplicas
                case n =>
                    obj -> selectReplicas(n, obj, clouds, distributionPlan)
            }
        })

        // the new plan does not contain unknown clouds
        assert(distributionPlan.values.flatten.toSet.subsetOf(cloudIds))
        // the new plan contains exactly the given buckets
        assert(distributionPlan.keySet == objects)
        // every bucket has the correct count of replicas
        assert(distributionPlan.values.forall(clouds => clouds.size
            == StorageSim.configuration.replicaCount))

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
                val load = cloudLoad(distributionPlan)
                val currentReplicas = distributionPlan.getOrElse(obj, Set.empty)
                val selection = selectReplicationTarget(obj, clouds, load, currentReplicas)
                val newDistributionPlan = distributionPlan + (obj -> (distributionPlan.getOrElse(obj, Set.empty) + selection))
                selectReplicas(count - 1, obj, clouds, newDistributionPlan)
        }

    private def cloudLoad(distributionPlan: Map[StorageObject, Set[Int]]): Map[Int, Double] = {
        val sizeDistribution = distributionPlan.foldLeft(Map.empty[Int, Double])((plan, b) => {
            val obj = b._1
            val clouds = b._2
            val sizeDist = clouds.map(c => c -> obj.size).groupBy(_._1).mapValues(_.map(_._2).sum)
            plan ++ sizeDist
        })

        val max = sizeDistribution.values.max
        sizeDistribution.mapValues(_ / max)
    }

    private def createActionPlan(
        distributionGoal: Map[StorageObject, Set[Int]],
        distributionState: Map[StorageObject, Set[Int]],
        activeTransactions: Set[DownloadRequest]): Map[Int, Load] = {

        val additionalCloudMap = distributionState.map(objectCloudMap => {
            val storageObject = objectCloudMap._1
            val currentClouds = objectCloudMap._2
            val additionalClouds = distributionGoal(storageObject) diff currentClouds
            storageObject -> additionalClouds
        })

        val additionalObjectMap = additionalCloudMap.toSeq.flatMap(objClouds => objClouds._2.map(objClouds._1 -> _)).
            groupBy(_._2).mapValues(_.map(_._1).toSet)

        additionalObjectMap.mapValues(objects => {
            val objectSourceMap = objects.map(obj => {
                obj -> RandomUtils.randomSelect1(distributionState(obj).toIndexedSeq)
            }).toMap
            Load(objectSourceMap)
        })
    }

    private def sendActions(cloud: Int, request: PlacementDialog): Unit = {
        val dialog = dialogCenter.openDialog(cloud)
        dialog.messageHandler = (content) => content match {
            case PlacementAck => dialog.close()
        }

        dialog.say(request, () => { throw new IllegalStateException })
    }
}