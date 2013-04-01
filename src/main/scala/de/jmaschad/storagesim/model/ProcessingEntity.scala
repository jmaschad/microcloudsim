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

object ProcessingEntity {
    val TimeResolution = 0.001
    private val Base = 10500
    val ProcUpdate = Base + 1
}

trait ProcessingEntity extends Entity {
    protected val bandwidth: Double
    protected lazy val processing = new ProcessingModel(log _, bandwidth)

    abstract override def startEntity() = {
        super.startEntity()
        scheduleUpdate()
    }

    abstract override def processEvent(event: SimEvent): Unit = event.getTag match {
        case ProcessingEntity.ProcUpdate =>
            processing.update()
            scheduleUpdate()

        case _ =>
            super.processEvent(event)
    }

    abstract override protected def reset() = {
        processing.reset()
        super.reset()
    }

    private def scheduleUpdate() =
        send(getId(), ProcessingEntity.TimeResolution, ProcessingEntity.ProcUpdate)
}