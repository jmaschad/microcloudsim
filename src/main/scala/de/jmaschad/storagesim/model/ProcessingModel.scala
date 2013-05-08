package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.CloudSim
import scala.collection.mutable.HashSet

object ProcessingModel extends SimEntity("ProcessingModel") {
    private val UpdateInterval = 0.001
    private val UpdateEvent = 10501

    var models = Set.empty[ProcessingModel]

    def createModel(procEntity: ProcessingEntity) = {
        val model = new ProcessingModel(procEntity)
        models += model
        model
    }

    def destroyModel(procModel: ProcessingModel) = {
        models -= procModel
    }

    override def startEntity() =
        scheduleUpdate()

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent) = event.getTag match {
        case UpdateEvent =>
            updateModels()
            scheduleUpdate()

        case _ =>
            throw new IllegalStateException
    }

    private def scheduleUpdate() = {
        schedule(getId, UpdateInterval, UpdateEvent)
    }

    private def updateModels() = {
        models foreach { _.progress() }
    }
}
import ProcessingModel._

class ProcessingModel(procEntity: ProcessingEntity) {
    private class Transfer(private var size: Double, val onFinish: () => Unit) {
        def progress(timespan: Double, bandwidth: Double): Unit =
            size -= (timespan * bandwidth)

        def isDone: Boolean = size < 1 * Units.Byte
    }

    private val uploads = HashSet.empty[Transfer]
    private val downloads = HashSet.empty[Transfer]

    def download(id: String, size: Double, onFinish: () => Unit): Unit =
        downloads += new Transfer(size, onFinish)

    def upload(id: String, size: Double, onFinish: () => Unit): Unit =
        uploads += new Transfer(size, onFinish)

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