package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.CloudSim
import scala.collection.mutable.HashSet
import de.jmaschad.storagesim.StatsCentral
import scala.collection.immutable.Queue

object ProcessingModel extends SimEntity("ProcessingModel") {
    private val UpdateInterval = 0.001
    private val StatsInterval = 1.0

    private val UpdateEvent = 10501
    private val StatsEvent = 10502

    var models = Map.empty[ProcessingEntity, ProcessingModel]

    var upStats = Map.empty[ProcessingEntity, Double]
    var allUp = IndexedSeq.empty[Int]
    var meanUp = 0.0

    var downStats = Map.empty[ProcessingEntity, Double]
    var allDown = IndexedSeq.empty[Int]
    var meanDown = 0.0

    def createModel(procEntity: ProcessingEntity) = {
        val model = new ProcessingModel(procEntity)
        models += procEntity -> model
        upStats += procEntity -> 0.0
        model
    }

    def destroyModel(procEntity: ProcessingEntity) = {
        models -= procEntity
        upStats -= procEntity
    }

    def loadDown(microCloud: Int): Double = {
        val cloud = CloudSim.getEntity(microCloud) match {
            case c: MicroCloud => c
            case _ => throw new IllegalStateException
        }

        assert(downStats.isDefinedAt(cloud))
        downStats(cloud) / meanDown
    }

    def allLoadDown(): IndexedSeq[Double] =
        allDown map { _ / meanDown }

    def loadUp(microCloud: Int): Double = {
        val cloud = CloudSim.getEntity(microCloud) match {
            case c: MicroCloud => c
            case _ => throw new IllegalStateException
        }

        assert(upStats.isDefinedAt(cloud))
        upStats(cloud) / meanUp
    }

    def allLoadUp(): IndexedSeq[Double] =
        allUp map { _ / meanUp }

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
            allUp = { microCloudModels map { _.loadUp } }.toIndexedSeq
            meanUp = allUp.sum / allUp.size.toDouble

            allDown = { microCloudModels map { _.loadDown } }.toIndexedSeq
            meanDown = allDown.sum / allDown.size.toDouble

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

    private def microCloudModels(): Set[ProcessingModel] =
        models.values collect { case m if m.procEntity.isInstanceOf[MicroCloud] => m } toSet
}
import ProcessingModel._

class ProcessingModel(val procEntity: ProcessingEntity) {
    private class Transfer(var size: Double, val onFinish: () => Unit) {
        def progress(timespan: Double, bandwidth: Double): Unit =
            size -= (timespan * bandwidth)

        def isDone: Boolean = size < 1 * Units.Byte
    }

    private var upCount = 0
    var loadUp = 0

    private var downCount = 0
    var loadDown = 0

    private val uploads = HashSet.empty[Transfer]
    private val downloads = HashSet.empty[Transfer]

    def download(id: String, size: Double, onFinish: () => Unit): Unit = {
        downloads += new Transfer(size, onFinish)
        downCount += 1
    }

    def upload(id: String, size: Double, onFinish: () => Unit): Unit = {
        uploads += new Transfer(size, onFinish)
        upCount += 1
    }

    def updateStats() = {
        loadUp = upCount
        upCount = 0

        loadDown = downCount
        downCount = 0
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