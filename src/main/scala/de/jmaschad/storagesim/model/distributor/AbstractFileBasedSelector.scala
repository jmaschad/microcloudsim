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
import org.cloudbus.cloudsim.core.CloudSim

abstract class AbstractFileBasedSelector(
    log: String => Unit,
    dialogEntity: DialogEntity) extends CloudSelector {

    // currently online clouds
    private var clouds = Set.empty[Int]

    // goal object cloud distribution
    private var distributionGoal = Map.empty[StorageObject, Set[Int]]

    // current object cloud distribution
    private var distributionState = Map.empty[StorageObject, Set[Int]]

    // current replication actions
    private var activeReplications = Map.empty[Int, Set[StorageObject]]
    private var startOfRepair = Double.NaN
    private var totalSizeOfRepair = Double.NaN

    override def initialize(microclouds: Set[MicroCloud], objects: Set[StorageObject], users: Set[User]): Unit = {
        clouds = microclouds map { _.getId() }
        distributionGoal = createDistributionPlan(objects)

        // initialization plan
        val allocationPlan = distributionGoal.foldLeft(Map.empty[Int, Set[StorageObject]]) {
            case (allocation, (obj, clouds)) =>
                allocation ++ clouds.map(cloud => cloud -> (allocation.getOrElse(cloud, Set.empty) + obj))
        }

        // we don't allocate to unknown clouds
        assert(allocationPlan.keySet.subsetOf(clouds))
        // we store exactly our objects, with the correct replica count
        assert(allocationPlan.foldLeft(Map.empty[StorageObject, Int]) {
            case (objectCount, (cloud, objects)) =>
                objectCount ++ objects.map(obj => obj -> (objectCount.getOrElse(obj, 0) + 1))
        } forall {
            _._2 == StorageSim.configuration.replicaCount
        })

        // initialization of the clouds
        val cloudIdMap = { microclouds map { cloud => cloud.getId -> cloud } toMap }
        allocationPlan foreach { case (cloudId, objects) => cloudIdMap(cloudId).initialize(objects) }

        // update the distribution state
        distributionState = distributionGoal
    }

    override def addCloud(cloud: Int): Unit =
        clouds += cloud

    override def removeCloud(cloud: Int): Unit = {
        val repairSize = {
            distributionState filter {
                case (obj, clouds) => clouds.contains(cloud)
            } map {
                case (obj, clouds) => obj.size
            } sum
        }
        if (activeReplications.isEmpty) {
            startOfRepair = CloudSim.clock()
            totalSizeOfRepair = repairSize
            log("Starting repair after failure of cloud %d [%.3fMB]".format(cloud, repairSize))
        } else {
            totalSizeOfRepair += repairSize
            log("Added repair of cloud %d [%.3fMB]".format(cloud, repairSize))
        }

        // update knowledge of current state
        assert(clouds.contains(cloud))
        clouds -= cloud
        distributionState = distributionState mapValues { _ - cloud }
        activeReplications -= cloud

        // throw if data was lost
        val lostObjects = distributionState filter { case (_, clouds) => clouds.isEmpty }
        if (lostObjects.nonEmpty) {
            log("An object was lost after a cloud failure. Time to dataloss %.3fs".format(CloudSim.clock()))
            CloudSim.terminateSimulation()
        }

        // create new distribution goal
        distributionGoal = createDistributionPlan()

        // create an action plan and inform the involved clouds
        val repairPlan = createRepairPlan()
        repairPlan foreach { case (cloud, load) => sendActions(cloud, load) }
    }

    def addedObject(cloud: Int, obj: StorageObject): Unit = {
        activeReplications.contains(cloud) match {
            case true if activeReplications(cloud).size == 1 =>
                activeReplications -= cloud

            case true =>
                activeReplications += cloud -> { activeReplications(cloud) - obj }

            case _ =>
                throw new IllegalStateException
        }

        if (activeReplications.isEmpty) {
            val duration = CloudSim.clock() - startOfRepair
            val meanBandwidth = (totalSizeOfRepair / duration) * 8
            log("Finished repair of %.3fMB in %.3s with a mean bandwidth of %.3fMbit/s".format(totalSizeOfRepair, duration, meanBandwidth))
            startOfRepair = Double.NaN
            totalSizeOfRepair = Double.NaN
        } else {
            val remainingObjects = activeReplications flatMap { case (_, objects) => objects }
            val remainingAmount = { remainingObjects map { _.size } sum }
            val duration = CloudSim.clock() - startOfRepair
            val currentMeanBandwidth = ((totalSizeOfRepair - remainingAmount) / duration) * 8
            log("repaired object [%.3fMB], %d objects remain [%.3fMB], repair mean bandwidth %.3fMbit/s".
                format(obj.size, remainingObjects.size, remainingAmount, currentMeanBandwidth))
        }
    }

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

    private def createDistributionPlan(initialObjects: Set[StorageObject] = Set.empty): Map[StorageObject, Set[Int]] = {
        val objects = if (initialObjects.nonEmpty)
            initialObjects
        else
            distributionState.keySet

        // remove unknown objects and clouds from the current plan
        var distributionPlan = distributionGoal.keys collect {
            case obj if objects.contains(obj) => obj -> distributionGoal(obj).intersect(clouds)
        } toMap

        // choose clouds for buckets which have too few replicas
        distributionPlan ++= objects map { obj =>
            val currentReplicas = distributionPlan.getOrElse(obj, Set.empty)
            val requiredTargetsCount = StorageSim.configuration.replicaCount - currentReplicas.size
            requiredTargetsCount match {
                case 0 =>
                    obj -> currentReplicas
                case n =>
                    obj -> selectReplicas(n, obj, clouds, distributionPlan)
            }
        }

        // the new plan does not contain unknown clouds
        assert(distributionPlan.values.flatten.toSet.subsetOf(clouds))
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

    private def createRepairPlan(): Map[Int, Load] = {
        // additional clouds per object
        val additionalCloudMap = distributionState map {
            case (obj, currentClouds) =>
                val additionalClouds = distributionGoal(obj) diff currentClouds
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