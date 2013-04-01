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
        def isDone: Boolean = size < 1 * Units.Byte
    }

    class Download(size: Double, onFinish: () => Unit) extends Transfer(size, onFinish) {
        def progress(timespan: Double): Transfer = new Download(size - (timespan * downloadBandwidth), onFinish)
    }

    class Upload(size: Double, onFinish: () => Unit) extends Transfer(size, onFinish) {
        def progress(timespan: Double): Transfer = new Upload(size - (timespan * uploadBandwidth), onFinish)
    }

    private var transfers = Set.empty[Transfer]
    private var uploadCount = 0
    private var downloadCount = 0

    def jobCount = transfers.size

    private def uploadBandwidth: Double = totalBandwidth / uploadCount
    private def downloadBandwidth: Double = totalBandwidth / downloadCount

    def download(size: Double, onFinish: () => Unit) = {
        downloadCount += 1
        add(new Download(size, onFinish))
    }

    def upload(size: Double, onFinish: () => Unit) = {
        uploadCount += 1
        add(new Upload(size, onFinish))
    }

    def update() =
        transfers = transfers.foldLeft(Set.empty[Transfer]) { (activeTransfers, outdatedTransfer) =>
            val updatedTransfer = outdatedTransfer.progress(ProcessingEntity.TimeResolution)
            if (updatedTransfer.isDone) {
                updatedTransfer.onFinish()
                updatedTransfer match {
                    case _: Download => downloadCount -= 1
                    case _: Upload => uploadCount -= 1
                }
                activeTransfers
            } else {
                activeTransfers + updatedTransfer
            }
        }

    def reset(): Unit = {
        transfers = Set.empty
        uploadCount = 0
        downloadCount = 0
    }

    private def add(transfer: Transfer) = transfers += transfer
}