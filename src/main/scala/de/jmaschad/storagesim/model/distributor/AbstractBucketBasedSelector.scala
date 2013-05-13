package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.MicroCloud
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.user.User
import org.cloudbus.cloudsim.core.CloudSim
import scala.util.Random
import de.jmaschad.storagesim.StatsCentral

abstract class AbstractBucketBasedSelector(
    val log: String => Unit,
    val dialogEntity: DialogEntity) extends CloudSelector {

    // currently online clouds
    private var clouds = Set.empty[Int]

    // goal bucket cloud distribution
    private var distributionGoal = Map.empty[String, Set[Int]]

    // current object cloud distribution
    var distributionState = Map.empty[StorageObject, Set[Int]]

    // current replication actions
    private var activeReplications = Map.empty[Int, Set[StorageObject]]

    override def initialize(microclouds: Set[MicroCloud], objects: Set[StorageObject], users: Set[User]) = {
        clouds = microclouds map { _.getId() }
        distributionGoal = createDistributionGoal(objects)

        // initialization plan
        val allocationPlan = distributionGoal.foldLeft(Map.empty[Int, Set[String]]) {
            case (allocation, (bucket, clouds)) =>
                allocation ++ (clouds map { cloud =>
                    cloud -> (allocation.getOrElse(cloud, Set.empty) + bucket)
                })
        }

        // we don't allocate to unknown clouds
        assert(allocationPlan.keySet.subsetOf(clouds))

        // we store all buckets, with the correct replica count
        assert(
            allocationPlan.foldLeft(Map.empty[String, Int]) { (bucketCount, allocation) =>
                val cloud = allocation._1
                val buckets = allocation._2
                bucketCount ++ buckets.map(bucket => bucket -> (bucketCount.getOrElse(bucket, 0) + 1))
            } forall { _._2 == StorageSim.configuration.replicaCount })

        // initialization of the clouds
        val bucketMap = objects groupBy { _.bucket }
        val idCloudMap = { microclouds map { cloud => cloud.getId -> cloud } toMap }
        allocationPlan foreach {
            case (cloudId, buckets) =>
                val cloudObjects = buckets flatMap { bucketMap(_) }
                idCloudMap(cloudId).initialize(cloudObjects)
        }

        // update the distribution state
        distributionState = {
            objects map { obj => obj -> distributionGoal(obj.bucket) }
        } toMap
    }

    override def optimizePlacement(): Unit = {}

    def addedObject(cloud: Int, obj: StorageObject): Unit = {
        // update the active replications
        activeReplications.contains(cloud) match {
            case true if activeReplications(cloud).size == 1 =>
                activeReplications -= cloud

            case true =>
                activeReplications += cloud -> { activeReplications(cloud) - obj }

            case _ =>
                throw new IllegalStateException
        }

        // check if a repair is finished
        if (activeReplications.isEmpty) {
            StatsCentral.finishRepair()
        } else {
            StatsCentral.progressRepair(obj.size)
        }

        // update the known distribution state
        distributionState += obj -> (distributionState.getOrElse(obj, Set.empty) + cloud)
    }

    def addCloud(cloud: Int) =
        clouds += cloud

    def removeCloud(cloud: Int) = {
        val repairSize = {
            distributionState filter {
                case (obj, clouds) => clouds.contains(cloud)
            } map {
                case (obj, clouds) => obj.size
            } sum
        }
        StatsCentral.startRepair(repairSize)

        // update knowledge of current state
        assert(clouds.contains(cloud))
        clouds -= cloud
        distributionState = distributionState mapValues { _ - cloud }
        activeReplications -= cloud

        // terminate if data was lost
        val lostObjects = distributionState filter { case (_, clouds) => clouds.isEmpty }
        if (lostObjects.nonEmpty) {
            log("An object was lost after a cloud failure. Time to dataloss %.3fs".format(CloudSim.clock()))
            CloudSim.terminateSimulation()
        }

        // create new distribution goal
        distributionGoal = createDistributionGoal()

        // create an action plan and inform the involved clouds
        val repairPlan = createRepairPlan()
        repairPlan foreach { case (cloud, load) => sendActions(cloud, load) }
    }

    protected def selectReplicas(bucket: String, currentReplicas: Set[Int], clouds: Set[Int]): Set[Int]

    private def createDistributionGoal(initialObjects: Set[StorageObject] = Set.empty): Map[String, Set[Int]] = {
        val bucketMap = distributionState.keySet groupBy { _.bucket }
        val buckets = if (initialObjects.nonEmpty) {
            initialObjects map { _.bucket } toSet
        } else {
            bucketMap keySet
        }

        // remove unknown buckets and clouds from the current plan
        var newGoal = distributionGoal.keys collect {
            case bucket if buckets.contains(bucket) =>
                bucket -> distributionGoal(bucket).intersect(clouds)
        } toMap

        // choose clouds for buckets which have too few replicas
        newGoal = buckets map { bucket =>
            val currentReplicas = newGoal.getOrElse(bucket, Set.empty)
            bucket -> selectReplicas(bucket, currentReplicas, clouds)
        } toMap

        // the new plan does not contain unknown clouds
        assert(newGoal.values.flatten.toSet.subsetOf(clouds))
        // the new plan contains exactly the given buckets
        assert(newGoal.keySet == buckets)
        // every bucket has the correct count of replicas
        assert(newGoal.values forall { clouds =>
            clouds.size == StorageSim.configuration.replicaCount
        })

        newGoal
    }

    private def createRepairPlan(): Map[Int, Load] = {
        // additional clouds per object
        val additionalCloudMap = distributionState map {
            case (obj, currentClouds) =>
                val additionalClouds = distributionGoal(obj.bucket) diff currentClouds
                obj -> additionalClouds
        }

        val objectCloudTuples = additionalCloudMap.toSeq flatMap {
            case (obj, clouds) =>
                clouds map { obj -> _ }
        }

        // additional objects per clouds
        val additionalObjectMap = objectCloudTuples groupBy {
            case (_, cloud) => cloud
        } mapValues { objectCloudTuples =>
            { objectCloudTuples unzip }._1 toSet
        }

        // remove already running replications
        val addedReplications = additionalObjectMap map {
            case (cloud, objects) =>
                cloud -> { objects -- activeReplications.getOrElse(cloud, Set.empty) }
        }

        // load instructions with random source selection
        val loadInstructions = addedReplications mapValues { objects =>
            Load(objects map { obj =>
                obj -> RandomUtils.randomSelect1(distributionState(obj).toIndexedSeq)
            } toMap)
        }

        // update the active replications
        activeReplications ++= loadInstructions map {
            case (cloud, load) =>
                cloud -> {
                    activeReplications.getOrElse(cloud, Set.empty[StorageObject]) ++ load.objSourceMap.keySet
                }
        }

        loadInstructions
    }

    private def sendActions(cloud: Int, request: PlacementDialog): Unit = {
        val dialog = dialogEntity.openDialog(cloud)
        dialog.messageHandler = (content) => content match {
            case PlacementAck => dialog.close()
        }

        dialog.say(request, () => { throw new IllegalStateException })
    }
}