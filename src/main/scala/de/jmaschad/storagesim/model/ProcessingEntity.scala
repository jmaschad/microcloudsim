package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.Downloader
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.StorageObject
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.predicates.PredicateType
import de.jmaschad.storagesim.model.processing.Uploader

abstract class ProcessingEntity(
    name: String,
    resources: ResourceCharacteristics,
    initialObjects: Iterable[StorageObject]) extends SimEntity(name) {
    protected val storageSystem = new StorageSystem(resources.storageDevices, initialObjects)
    protected val processing = new ProcessingModel(log _, scheduleProcessingUpdate _, resources.bandwidth)

    protected val downloader = new Downloader(send _, log _)
    protected val uploader = new Uploader(send _, log _)

    def startEntity(): Unit = {}

    final def processEvent(event: SimEvent): Unit = event.getTag match {
        case ProcessingModel.ProcUpdate =>
            processing.update()

        case Downloader.Download =>
            downloader.process(event.getSource(), event.getData())

        case Uploader.Upload =>
            uploader.process(event.getSource, event.getData)

        case _ =>
            if (!process(event)) log("dropped event " + event)
    }

    def shutdownEntity(): Unit

    def log(message: String): Unit

    protected def process(event: SimEvent): Boolean

    protected def resetModel() = {
        uploader.killed()
        downloader.killed()

        processing.reset()
        storageSystem.reset()
    }

    private def scheduleProcessingUpdate(delay: Double) = {
        CloudSim.cancel(getId(), new PredicateType(ProcessingModel.ProcUpdate))
        send(getId(), delay, ProcessingModel.ProcUpdate)
    }

}