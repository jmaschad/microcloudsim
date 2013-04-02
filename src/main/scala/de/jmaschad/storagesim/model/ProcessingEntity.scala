package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.transfer.Message
import de.jmaschad.storagesim.model.transfer.Timeout
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.transfer.Dialog
import de.jmaschad.storagesim.Units

object ProcessingEntity {
    val TimeResolution = 0.001
    private val Base = 10500
    val ProcUpdate = Base + 1
}

trait ProcessingEntity extends Entity {
    protected val bandwidth: Double

    def download(size: Double, onFinish: () => Unit) = {
        downloadCount += 1
        transfers += new Download(size, onFinish)
    }

    def upload(size: Double, onFinish: () => Unit) = {
        uploadCount += 1
        transfers += new Upload(size, onFinish)
    }

    def jobCount = transfers.size

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
        transfers = Set.empty
        uploadCount = 0
        downloadCount = 0
        super.reset()
    }

    private abstract class Transfer(size: Double, val onFinish: () => Unit) {
        def progress(timespan: Double): Transfer
        def finish(): Unit
        def isDone: Boolean = size < 1 * Units.Byte
    }

    private class Download(size: Double, onFinish: () => Unit) extends Transfer(size, onFinish) {
        def progress(timespan: Double): Transfer = new Download(size - (timespan * downloadBandwidth), onFinish)
        def finish() = {
            onFinish()
            downloadCount -= 1
            assert(downloadCount >= 0)
        }
    }

    private class Upload(size: Double, onFinish: () => Unit) extends Transfer(size, onFinish) {
        def progress(timespan: Double): Transfer = new Upload(size - (timespan * uploadBandwidth), onFinish)
        def finish() = {
            onFinish()
            uploadCount -= 1
            assert(uploadCount >= 0)
        }
    }

    private var transfers = Set.empty[Transfer]
    private var uploadCount = 0
    private var downloadCount = 0

    private def uploadBandwidth: Double = bandwidth / uploadCount
    private def downloadBandwidth: Double = bandwidth / downloadCount

    private def scheduleUpdate() =
        send(getId(), ProcessingEntity.TimeResolution, ProcessingEntity.ProcUpdate)

    private def update() =
        transfers = transfers.foldLeft(Set.empty[Transfer]) { (activeTransfers, outdatedTransfer) =>
            val updatedTransfer = outdatedTransfer.progress(ProcessingEntity.TimeResolution)
            updatedTransfer.isDone match {
                case true =>
                    updatedTransfer.finish()
                    activeTransfers

                case false =>
                    activeTransfers + updatedTransfer
            }
        }
}