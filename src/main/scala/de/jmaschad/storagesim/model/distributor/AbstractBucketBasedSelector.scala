package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck

abstract class AbstractBucketBasedSelector(
    val log: String => Unit,
    val dialogCenter: DialogCenter) extends CloudSelector {

    private var clouds = Set.empty[Int]
    private var distributionGoal = Map.empty[String, Set[Int]]
    private var distributionState = Map.empty[StorageObject, Set[Int]]

    private var activeOperations = Set.empty[RepairTracker]
    private var activeDownloads = Set.empty[DownloadRequest]

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

    def addCloud(cloud: Int) = {
        clouds += cloud
    }

    def removeCloud(cloud: Int) = {
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

        // The current set of objects of all clouds, ordered by bucket  
        val buckets = distributionState.keySet.foldLeft(Set.empty[String])((buckets, obj) => buckets + obj.bucket)

        // create new distribution goal
        distributionGoal = createDistributionPlan(clouds, buckets, distributionGoal)

        // create an action plan and inform the involved clouds
        val actionPlan = createActionPlan(distributionGoal, distributionState, activeDownloads)
        val newDownloads = actionPlan.flatMap(m => {
            m._2.objSourceMap.keys.map(DownloadRequest(m._1, _))
        }).toSet
        activeOperations += new RepairTracker(log, newDownloads)

        actionPlan.foreach(cloudPlan => sendActions(cloudPlan._1, cloudPlan._2))
    }

    protected def createDistributionPlan(
        cloudIds: Set[Int],
        buckets: Set[String],
        currentPlan: Map[String, Set[Int]] = Map.empty): Map[String, Set[Int]]

    protected def getDistributionState: Map[StorageObject, Set[Int]] = distributionState

    private def createActionPlan(
        distributionGoal: Map[String, Set[Int]],
        distributionState: Map[StorageObject, Set[Int]],
        activeTransactions: Set[DownloadRequest]): Map[Int, Load] = {

        val additionalCloudMap = distributionState.map(objectCloudMap => {
            val storageObject = objectCloudMap._1
            val currentClouds = objectCloudMap._2
            val additionalClouds = distributionGoal(storageObject.bucket) diff currentClouds
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