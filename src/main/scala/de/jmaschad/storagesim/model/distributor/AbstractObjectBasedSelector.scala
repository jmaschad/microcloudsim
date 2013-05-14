package de.jmaschad.storagesim.model.distributor

import de.jmaschad.storagesim.model.transfer.dialogs.PlacementDialog
import de.jmaschad.storagesim.RandomUtils
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary
import de.jmaschad.storagesim.model.transfer.dialogs.Load
import de.jmaschad.storagesim.model.MicroCloud
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.PlacementAck
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._
import de.jmaschad.storagesim.StorageSim
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.StorageObject
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.user.User
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.MicroCloud
import scala.util.Random
import de.jmaschad.storagesim.StatsCentral
import de.jmaschad.storagesim.model.transfer.dialogs.Load

abstract class AbstractObjectBasedSelector(
    log: String => Unit,
    dialogEntity: DialogEntity) extends CloudSelector {

    // currently online clouds
    private var clouds = Set.empty[Int]

    // goal object cloud distribution
    var distributionGoal = Map.empty[StorageObject, Set[Int]]

    // current object cloud distribution
    var distributionState = Map.empty[StorageObject, Set[Int]]

    // current replication actions
    private var activeReplications = Map.empty[Int, Set[StorageObject]]

    // current migration actions
    case class Migration(obj: StorageObject, source: Int)
    var activeMigrations = Map.empty[Int, Set[Migration]]

    override def initialize(microclouds: Set[MicroCloud], objects: Set[StorageObject], users: Set[User]): Unit = {
        clouds = microclouds map { _.getId() }
        distributionGoal = createDistributionGoal(objects)

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
        StatsCentral.startRepair(repairSize)

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
        } else {
            // create new distribution goal
            distributionGoal = createDistributionGoal()

            // create an action plan and inform the involved clouds
            val repairPlan = createRepairPlan()
            repairPlan foreach { case (cloud, load) => sendAction(cloud, load) }
        }
    }

    override def optimizePlacement(): Unit = {}

    def addedObject(cloud: Int, obj: StorageObject): Unit = {
        val migration = activeMigrations.getOrElse(cloud, Set.empty) filter { _.obj == obj }
        if (migration.nonEmpty) {
            assert(migration.size == 1)
            completeMigration(cloud, migration.head)
        } else if (activeReplications.contains(cloud)) {
            completeReplication(cloud, obj)

            // check if a repair is finished
            if (activeReplications.isEmpty) {
                StatsCentral.finishRepair()
            } else {
                StatsCentral.progressRepair(obj.size)
            }
        }

        // update the known distribution state
        distributionState += obj -> (distributionState.getOrElse(obj, Set.empty) + cloud)
    }

    protected def selectReplicas(obj: StorageObject, currentReplicas: Set[Int], clouds: Set[Int]): Set[Int]

    protected def selectRepairSource(obj: StorageObject): Int

    protected def isRepairing() = activeReplications.nonEmpty

    protected def migrate(obj: StorageObject, from: Int, to: Int): Unit = {
        assert(!isRepairing)
        assert(from != StorageSim.failingCloud && to != StorageSim.failingCloud)
        distributionGoal += obj -> { distributionState(obj) - from + to }
        activeMigrations += to -> { activeMigrations.getOrElse(to, Set.empty) + Migration(obj, from) }
        sendAction(to, Load(Map(obj -> from)))
    }

    private def completeMigration(to: Int, migration: Migration): Unit = {
        distributionState += migration.obj -> { distributionState(migration.obj) - migration.source + to }

        // little shortcut
        val microCloud = Entity.entityForId(migration.source) match {
            case mc: MicroCloud => mc
            case _ => throw new IllegalStateException
        }
        microCloud.objects -= migration.obj
        // --

        activeMigrations(to) size match {
            case 1 =>
                activeMigrations -= to
            case n =>
                activeMigrations += to -> { activeMigrations(to) - migration }
        }
    }

    private def completeReplication(cloud: Int, obj: StorageObject): Unit =
        activeReplications(cloud).size match {
            case 1 =>
                activeReplications -= cloud

            case n =>
                activeReplications += cloud -> { activeReplications(cloud) - obj }
        }

    private def createDistributionGoal(initialObjects: Set[StorageObject] = Set.empty): Map[StorageObject, Set[Int]] = {
        val objects = if (initialObjects.nonEmpty)
            initialObjects
        else
            distributionState.keySet

        // remove unknown objects and clouds from the current plan
        var newGoal = distributionGoal.keys collect {
            case obj if objects.contains(obj) => obj -> distributionGoal(obj).intersect(clouds)
        } toMap

        // choose clouds for buckets which have too few replicas
        newGoal = objects map { obj =>
            val currentReplicas = newGoal.getOrElse(obj, Set.empty)
            obj -> selectReplicas(obj, currentReplicas, clouds)
        } toMap

        // the new plan does not contain unknown clouds
        assert(newGoal.values.flatten.toSet.subsetOf(clouds))
        // the new plan contains exactly the given buckets
        assert(newGoal.keySet == objects)
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
                val additionalClouds = distributionGoal(obj) -- currentClouds
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
                cloud -> { objects -- activeReplications.getOrElse(cloud, Set.empty) -- { activeMigrations.getOrElse(cloud, Set.empty) map { _.obj } } }
        }

        // load instructions with random source selection
        val loadInstructions = addedReplications mapValues { objects =>
            Load(objects map { obj =>
                obj -> selectRepairSource(obj)
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

    protected def sendAction(cloud: Int, request: PlacementDialog): Unit = {
        val dialog = dialogEntity.openDialog(cloud)
        dialog.messageHandler = (content) => content match {
            case PlacementAck => dialog.close()
        }

        dialog.say(request, () => { throw new IllegalStateException })
    }
}