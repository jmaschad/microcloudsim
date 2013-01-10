
package de.jmaschad.storagesim

import scala.util.Random
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.distribution.RealDistribution
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.Disposer
import de.jmaschad.storagesim.model.User
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.microcloud.MicroCloudResourceCharacteristics
import de.jmaschad.storagesim.model.storage.StorageDevice
import de.jmaschad.storagesim.model.storage.StorageObject
import java.util.Calendar
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.model.distribution.RequestDistributor
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import de.jmaschad.storagesim.model.request.GetRequest
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.request.GetRequest
import de.jmaschad.storagesim.model.request.GetRequest

object StorageSim {
    private val log = Log.line("StorageSim", _: String)

    val simDuration = 10

    def main(args: Array[String]) {
        CloudSim.init(1, Calendar.getInstance(), false)

        val distributor = RequestDistributor.randomRequestDistributor
        val userCount = 10000

        val cloudCount = 100
        val storageDevicePerCloud = 10

        val bucketCountDist = new NormalDistribution(10, 2)
        val objectCountDist = new NormalDistribution(100, 20)
        val objectSizeDist = new NormalDistribution(1 * Units.MByte, 200 * Units.KByte)

        log("create disposer")
        val disposer = createDisposer(distributor, simDuration)

        log("create users")
        val users = createUsers(userCount, disposer)

        log("create objects")
        val objects = createObjects(bucketCountDist, objectCountDist, objectSizeDist, users)

        log("add user behavior")
        users.foreach(user => {
            val userObjects = objects(user)
            val delayModel = new NormalDistribution(1.0, 0.3)
            val objectSelectionModel = new UniformIntegerDistribution(0, userObjects.size - 1)
            val behavior = Behavior(delayModel, objectSelectionModel, userObjects, (storageObject, time) => { new GetRequest(user, storageObject, time) })
            user.addBehavior(behavior)
        })

        log("create clouds")
        val bucketObjectsMap = objects.values.flatten.groupBy(_.bucket)
        val clouds = createMicroClouds(cloudCount, storageDevicePerCloud, bucketObjectsMap, disposer)

        log("will start simulation")
        CloudSim.startSimulation();
    }

    private def createDisposer(distributor: RequestDistributor, simulationDuration: Double): Disposer = {
        val disposer = new Disposer("dp", distributor)
        CloudSim.send(0, disposer.getId(), simulationDuration + 1, Disposer.Shutdown, null)
        disposer
    }

    private def createUsers(userCount: Int, disposer: Disposer): Seq[User] =
        for (i <- 1 to userCount) yield new User("u" + i, disposer)

    private def createObjects(bucketCountDist: RealDistribution, objectCountDist: RealDistribution, sizeDist: RealDistribution, users: Seq[User]): Map[User, IndexedSeq[StorageObject]] =
        users.map(user => {
            val objectCount = objectCountDist.sample().toInt.max(1)
            val bucketCount = bucketCountDist.sample().toInt.max(1)

            def createObject(idx: Int) = new StorageObject(
                "obj" + idx,
                "bucket-" + user.getName + "-" + Random.nextInt(bucketCount),
                sizeDist.sample().max(1 * Units.Byte))

            user -> (1 to objectCount).map(createObject).toIndexedSeq
        }).toMap

    private def createMicroClouds(cloudCount: Int, storageDeviceCount: Int, bucketObjectsMap: Map[String, Iterable[StorageObject]], disposer: Disposer): Seq[MicroCloud] = {
        val buckets = bucketObjectsMap.keys.toIndexedSeq
        val bucketGroupIndices = buckets.indices.groupBy(_ % cloudCount)

        assert(bucketGroupIndices.size == cloudCount, "expected %d, found %d.".format(cloudCount, bucketGroupIndices.size))

        for (i <- 0 until cloudCount) yield {
            val storageDevices = for (i <- 1 to storageDeviceCount)
                yield new StorageDevice(capacity = 2 * Units.TByte, bandwidth = 600 * Units.MByte)
            val charact = new MicroCloudResourceCharacteristics(bandwidth = 125 * Units.MByte, storageDevices)
            val objects = bucketGroupIndices(i).map(idx => bucketObjectsMap(buckets(idx))).flatten
            new MicroCloud("mc" + (i + 1), charact, objects, disposer)
        }
    }

}