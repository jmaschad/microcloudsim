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
import de.jmaschad.storagesim.model.transfer.Dialog

trait ProcessingEntity extends SimEntity {
    protected val bandwidth: Double
    protected lazy val processing = new ProcessingModel(log _, scheduleProcessingUpdate _, bandwidth)

    private var lastUpdate: Option[SimEvent] = None

    abstract override def processEvent(event: SimEvent): Unit = event.getTag match {
        case ProcessingModel.ProcUpdate =>
            processing.update()

        case _ =>
            super.processEvent(event)
    }

    protected def log(message: String): Unit

    protected def reset() = {
        processing.reset()
    }

    private def scheduleProcessingUpdate(delay: Double) = {
        lastUpdate.foreach(CloudSim.cancel(_))
        lastUpdate = Some(send(getId(), delay, ProcessingModel.ProcUpdate))
    }
}