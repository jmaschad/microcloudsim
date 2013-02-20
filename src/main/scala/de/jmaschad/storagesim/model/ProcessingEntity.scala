package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.transfer.Message
import de.jmaschad.storagesim.model.transfer.Timeout
import de.jmaschad.storagesim.model.transfer.DialogCenter

object ProcessingEntity {
    private val Base = 30000
    val DialogMessage = Base + 1
    val DialogTimeout = DialogMessage + 1
}

abstract class ProcessingEntity(
    name: String,
    resources: ResourceCharacteristics) extends SimEntity(name) {

    protected var storageSystem = new StorageSystem(log _, resources.storageDevices)
    protected var processing = new ProcessingModel(log _, scheduleProcessingUpdate _, resources.bandwidth)
    protected var dialogCenter = new DialogCenter(this, createMessageHandler _, send _)

    private var lastUpdate: Option[SimEvent] = None

    override def startEntity(): Unit = {}

    override def shutdownEntity(): Unit

    final override def processEvent(event: SimEvent): Unit = event.getTag match {
        case ProcessingEntity.DialogMessage =>
            val message = event.getData() match {
                case m: Message => m
                case _ => throw new IllegalStateException
            }
            dialogCenter.handleMessage(event.getSource(), message)

        case ProcessingEntity.DialogTimeout =>
            val timeout = event.getData match {
                case t: Timeout => t
                case _ => throw new IllegalStateException
            }
            dialogCenter.handleTimeout(timeout)

        case ProcessingModel.ProcUpdate =>
            processing.update()

        case _ =>
            process(event)
    }

    protected def log(message: String): Unit

    protected def process(event: SimEvent): Unit

    protected def createMessageHandler(source: Int, message: Message): Option[DialogCenter.MessageHandler]

    protected def resetModel() = {
        dialogCenter = new DialogCenter(this, createMessageHandler _, send _)
        processing = processing.reset()
        storageSystem = storageSystem.reset()
    }

    private def scheduleProcessingUpdate(delay: Double) = {
        lastUpdate.foreach(CloudSim.cancel(_))
        lastUpdate = Some(send(getId(), delay, ProcessingModel.ProcUpdate))
    }
}