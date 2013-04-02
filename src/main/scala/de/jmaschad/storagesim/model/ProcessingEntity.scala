package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.Units

object ProcessingEntity {
    val TimeResolution = 0.001
    private val Base = 10500
    val ProcUpdate = Base + 1
}

trait ProcessingEntity extends Entity {
    protected val bandwidth: Double

    def download(size: Double, onFinish: () => Unit) =
        downloads += new Transfer(size, onFinish)

    def upload(size: Double, onFinish: () => Unit) =
        uploads += new Transfer(size, onFinish)

    abstract override def startEntity() = {
        super.startEntity()
        scheduleUpdate()
    }

    abstract override def processEvent(event: SimEvent): Unit = event.getTag match {
        case ProcessingEntity.ProcUpdate =>
            update()
            scheduleUpdate()

        case _ =>
            super.processEvent(event)
    }

    abstract override protected def reset() = {
        uploads = Set.empty
        downloads = Set.empty
        super.reset()
    }

    private class Transfer(size: Double, val onFinish: () => Unit) {
        def progress(timespan: Double, bandwidth: Double): Transfer = new Transfer(size - (timespan * bandwidth), onFinish)
        def isDone: Boolean = size < 1 * Units.Byte
    }

    private var uploads = Set.empty[Transfer]
    private var downloads = Set.empty[Transfer]

    private def scheduleUpdate() =
        send(getId(), ProcessingEntity.TimeResolution, ProcessingEntity.ProcUpdate)

    private def update() = {
        def updateWithBandwidth(transfers: Set[Transfer], bandwidth: Double): Set[Transfer] =
            transfers.foldLeft(Set.empty[Transfer]) { (activeTransfers, outdatedTransfer) =>
                val updatedTransfer = outdatedTransfer.progress(ProcessingEntity.TimeResolution, bandwidth)
                updatedTransfer.isDone match {
                    case true =>
                        updatedTransfer.onFinish()
                        activeTransfers

                    case false =>
                        activeTransfers + updatedTransfer
                }
            }

        uploads = updateWithBandwidth(uploads, bandwidth / uploads.size)
        downloads = updateWithBandwidth(downloads, bandwidth / downloads.size)
    }
}