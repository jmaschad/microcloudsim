package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.CloudSim
import scala.collection.mutable.HashSet
import de.jmaschad.storagesim.StatsCentral
import scala.collection.immutable.Queue
import scala.math._

import de.jmaschad.storagesim.model.user.User

object ProcessingModel extends SimEntity("ProcessingModel") {
    private val UpdateInterval = 0.001
    private val StatsInterval = 1.0

    private val UpdateEvent = 10501
    private val StatsEvent = 10502

    var models = Map.empty[ProcessingEntity, ProcessingModel]

    var upStats = Map.empty[MicroCloud, Map[StorageObject, Double]]
    var userUpStats = Map.empty[User, Map[StorageObject, Double]]
    var meanUp = 0.0

    var downStats = Map.empty[MicroCloud, Map[StorageObject, Double]]
    var userDownStats = Map.empty[User, Map[StorageObject, Double]]
    var meanDown = 0.0

    def createModel(procEntity: ProcessingEntity) = {
        val model = new ProcessingModel(procEntity)
        models += procEntity -> model

        model
    }

    def destroyModel(procEntity: ProcessingEntity) = {
        models -= procEntity
    }

    def loadDown(microCloud: Int): Map[StorageObject, Double] = {
        val cloud = CloudSim.getEntity(microCloud) match {
            case c: MicroCloud => c
            case _ => throw new IllegalStateException
        }

        assert(downStats.isDefinedAt(cloud))
        downStats(cloud)
    }

    def allLoadDown(): Iterable[Double] =
        downStats.values map { _.values sum }

    def loadUp(microCloud: Int): Map[StorageObject, Double] = {
        val cloud = CloudSim.getEntity(microCloud) match {
            case c: MicroCloud => c
            case _ => throw new IllegalStateException
        }

        assert(upStats.isDefinedAt(cloud))
        upStats(cloud)
    }

    def allLoadUp(): Iterable[Double] =
        upStats.values map { _.values sum }

    override def startEntity() = {
        scheduleUpdate()
        scheduleStats()
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent) = event.getTag match {
        case UpdateEvent =>
            updateModels()
            scheduleUpdate()

        case StatsEvent =>
            updateStats()

            upStats = microCloudModels mapValues { model => model.loadUp }
            userUpStats = userModels mapValues { model => model.loadUp }

            downStats = microCloudModels mapValues { model => model.loadDown }
            userDownStats = userModels mapValues { model => model.loadDown }

            scheduleStats()

        case _ =>
            throw new IllegalStateException
    }

    private def scheduleUpdate() = {
        schedule(getId, UpdateInterval, UpdateEvent)
    }

    private def scheduleStats() = {
        schedule(getId, StatsInterval, StatsEvent)
    }

    private def updateModels() = {
        models.values foreach { _.progress() }
    }

    private def updateStats() = {
        models.values foreach { _.updateStats() }
    }

    private def microCloudModels(): Map[MicroCloud, ProcessingModel] =
        models.keys collect { case cloud: MicroCloud if cloud.isOnline => cloud -> models(cloud) } toMap

    private def userModels(): Map[User, ProcessingModel] =
        models.keys collect { case user: User => user -> models(user) } toMap
}
import ProcessingModel._

class ProcessingModel(val procEntity: ProcessingEntity) {
    private class Transfer(var size: Double, val onFinish: () => Unit) {
        def progress(timespan: Double, bandwidth: Double): Unit =
            size -= (timespan * bandwidth)

        def isDone: Boolean = size < 1 * Units.Byte
    }

    private var upAmount = Map.empty[StorageObject, Double]
    var loadUp = Map.empty[StorageObject, Double]

    private var downAmount = Map.empty[StorageObject, Double]
    var loadDown = Map.empty[StorageObject, Double]

    private val uploads = HashSet.empty[Transfer]
    private val downloads = HashSet.empty[Transfer]

    def download(obj: StorageObject, size: Double, onFinish: () => Unit): Unit = {
        downloads += new Transfer(size, onFinish)
        downAmount += obj -> { downAmount.getOrElse(obj, 0.0) + size }
    }

    def upload(obj: StorageObject, size: Double, onFinish: () => Unit): Unit = {
        uploads += new Transfer(size, onFinish)
        upAmount += obj -> { upAmount.getOrElse(obj, 0.0) + size }
    }

    def updateStats() = procEntity match {
        case _: MicroCloud =>
            loadUp = upAmount
            upAmount = Map.empty

            loadDown = downAmount
            downAmount = Map.empty

        case _: User if (CloudSim.clock % 10) == 0 =>
            loadUp = upAmount
            upAmount = Map.empty

            loadDown = downAmount
            downAmount = Map.empty

        case _ =>
    }

    def progress() = {
        if (uploads.nonEmpty) {
            val uploadBandwidth = procEntity.bandwidth / uploads.size
            for (ul <- uploads) {
                ul.progress(UpdateInterval, uploadBandwidth)
                if (ul.isDone) {
                    ul.onFinish()
                    uploads -= ul
                }
            }
        }

        if (downloads.nonEmpty) {
            val downloadBandwidth = procEntity.bandwidth / downloads.size
            for (dl <- downloads) {
                dl.progress(UpdateInterval, downloadBandwidth)
                if (dl.isDone) {
                    dl.onFinish()
                    downloads -= dl
                }
            }
        }
    }
}