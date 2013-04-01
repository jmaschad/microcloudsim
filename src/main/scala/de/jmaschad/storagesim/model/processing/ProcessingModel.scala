package de.jmaschad.storagesim.model.processing

import scala.Array.canBuildFrom
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.Units
import de.jmaschad.storagesim.model.ProcessingEntity

class ProcessingModel(
    log: String => Unit,
    totalBandwidth: Double) {

    abstract class Transfer(size: Double, val onFinish: () => Unit) {
        def progress(timespan: Double): Transfer
        def finish(): Unit
        def isDone: Boolean = size < 1 * Units.Byte
    }

    class Download(size: Double, onFinish: () => Unit) extends Transfer(size, onFinish) {
        def progress(timespan: Double): Transfer = new Download(size - (timespan * downloadBandwidth), onFinish)
        def finish() = {
            onFinish()
            downloadCount -= 1
            assert(downloadCount >= 0)
        }
    }

    class Upload(size: Double, onFinish: () => Unit) extends Transfer(size, onFinish) {
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

    def jobCount = transfers.size

    private def uploadBandwidth: Double = totalBandwidth / uploadCount
    private def downloadBandwidth: Double = totalBandwidth / downloadCount

    def download(size: Double, onFinish: () => Unit) = {
        downloadCount += 1
        transfers += new Download(size, onFinish)
    }

    def upload(size: Double, onFinish: () => Unit) = {
        uploadCount += 1
        transfers += new Upload(size, onFinish)
    }

    def update() =
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

    def reset(): Unit = {
        transfers = Set.empty
        uploadCount = 0
        downloadCount = 0
    }
}