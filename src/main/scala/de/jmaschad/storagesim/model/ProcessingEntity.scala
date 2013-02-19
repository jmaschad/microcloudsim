package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.predicates.PredicateType

import de.jmaschad.storagesim.model.processing.Downloader
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.Uploader

abstract class ProcessingEntity(
    name: String,
    resources: ResourceCharacteristics) extends SimEntity(name) {

    protected var storageSystem = new StorageSystem(log _, resources.storageDevices)
    protected var processing = new ProcessingModel(log _, scheduleProcessingUpdate _, resources.bandwidth)
    protected var downloader = new Downloader(send _, log _, getId)
    protected var uploader = new Uploader(send _, log _, getId)

    private var lastUpdate: Option[SimEvent] = None

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
        uploader = uploader.reset()
        downloader = downloader.reset()
        processing = processing.reset()
        storageSystem = storageSystem.reset()
    }

    def scheduleProcessingUpdate(delay: Double) = {
        lastUpdate.foreach(CloudSim.cancel(_))
        lastUpdate = Some(send(getId(), delay, ProcessingModel.ProcUpdate))
    }
}