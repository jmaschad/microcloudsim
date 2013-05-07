package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.CloudSim

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

class ProcessingModel(procEntity: ProcessingEntity) {
    private class Transfer(val id: String, size: Double, val onFinish: () => Unit) {
        def progress(timespan: Double, bandwidth: Double): Transfer = new Transfer(id, size - (timespan * bandwidth), onFinish)
        def isDone: Boolean = size < 1 * Units.Byte
    }

    private var uploads = Set.empty[Transfer]
    private var downloads = Set.empty[Transfer]

    def download(id: String, size: Double, onFinish: () => Unit) =
        downloads += new Transfer(id, size, onFinish)

    def upload(id: String, size: Double, onFinish: () => Unit) =
        uploads += new Transfer(id, size, onFinish)

    def progress() = {
        if (uploads.nonEmpty) {
            val uploadBandwidth = procEntity.bandwidth / uploads.size
            uploads = uploads map { _.progress(ProcessingModel.UpdateInterval, uploadBandwidth) }
            val finishedUploads = uploads filter { _.isDone }
            finishedUploads foreach { _.onFinish() }
            uploads --= finishedUploads
        }

        if (downloads.nonEmpty) {
            val downloadBandwidth = procEntity.bandwidth / downloads.size
            downloads = downloads map { _.progress(ProcessingModel.UpdateInterval, downloadBandwidth) }
            val finishedDownloads = downloads filter { _.isDone }
            finishedDownloads foreach { _.onFinish() }
            downloads --= finishedDownloads
        }
    }
}