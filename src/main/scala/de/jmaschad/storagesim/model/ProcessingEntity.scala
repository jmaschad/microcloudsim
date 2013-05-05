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
import ProcessingEntity._

trait ProcessingEntity extends Entity {
    protected val bandwidth: Double
    private var uploads = Set.empty[Transfer]
    private var downloads = Set.empty[Transfer]

    private class Transfer(val id: String, size: Double, val onFinish: () => Unit) {
        def progress(timespan: Double, bandwidth: Double): Transfer = new Transfer(id, size - (timespan * bandwidth), onFinish)
        def isDone: Boolean = size < 1 * Units.Byte
    }

    def download(id: String, size: Double, onFinish: () => Unit) =
        downloads += new Transfer(id, size, onFinish)

    def upload(id: String, size: Double, onFinish: () => Unit) =
        uploads += new Transfer(id, size, onFinish)

    abstract override def startEntity() = {
        super.startEntity()
        send(getId(), TimeResolution, ProcUpdate)
    }

    abstract override def processEvent(event: SimEvent): Unit = event.getTag match {
        case ProcessingEntity.ProcUpdate =>
            if (uploads.nonEmpty) {
                val uploadBandwidth = bandwidth / uploads.size
                uploads = uploads map { _.progress(TimeResolution, uploadBandwidth) }
                val finishedUploads = uploads filter { _.isDone }
                finishedUploads foreach { _.onFinish() }
                uploads --= finishedUploads
            }

            if (downloads.nonEmpty) {
                val downloadBandwidth = bandwidth / downloads.size
                downloads = downloads map { _.progress(TimeResolution, downloadBandwidth) }
                val finishedDownloads = downloads filter { _.isDone }
                finishedDownloads foreach { _.onFinish() }
                downloads --= finishedDownloads
            }

            send(getId(), TimeResolution, ProcUpdate)

        case _ =>
            super.processEvent(event)
    }

    abstract override protected def reset() = {
        uploads = Set.empty
        downloads = Set.empty
        super.reset()
    }
}