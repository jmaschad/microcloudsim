package de.jmaschad.storagesim.model.processing

import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.CloudSim
import scala.util.Random

object Downloader {
    val DownloadMaxTime = 2.0

    private val Base = 10400
    val Download = Base + 1
}
import Downloader._

class Downloader(
    send: (Int, Double, Int, Object) => Unit,
    log: String => Unit,
    entityId: Int) {

    var downloads = Map.empty[String, Transfer]
    var waitingForFinish = Map.empty[String, Transfer]

    def start(transferId: String, size: Double, source: Int, process: (Double, () => Unit) => Unit, onFinish: Boolean => Unit) = {
        assert(source != entityId)

        log("adding download " + transferId)
        val transfer = new Transfer(source, Transfer.packetSize(size), Transfer.packetCount(size), process, onFinish)
        downloads += transferId -> transfer
        scheduleTimeout(transferId, transfer.packetNumber)

        TransferProbe.add(transferId, size)
    }

    def process(source: Int, request: Object) = request match {
        case Packet(transferId, nr, size) =>
            packetReceived(transferId, nr, size)

        case FinishDownload(transferId) =>
            log("download from " + CloudSim.getEntityName(source) +
                " completed. " + TransferProbe.finish(transferId))
            waitingForFinish(transferId).onFinish(true)
            waitingForFinish -= transferId

        case DownloadTimeout(run) => run()

        case _ => throw new IllegalStateException("request error")
    }

    def reset(): Downloader =
        new Downloader(send, log, entityId)

    private def packetReceived(transferId: String, nr: Int, size: Double) = {
        val transfer = downloads(transferId)
        assert(nr == transfer.packetNumber)

        transfer.nextPacket() match {
            case Some(newTracker) =>
                downloads += transferId -> newTracker
                transfer.process(size, () => {
                    // ack
                    send(transfer.partner, 0.0, Uploader.Upload, Ack(transferId, nr))
                    // timeout
                    scheduleTimeout(transferId, newTracker.packetNumber)
                })

            case None =>
                downloads -= transferId
                waitingForFinish += transferId -> transfer
                transfer.process(size, () => {
                    // ack
                    send(transfer.partner, 0.0, Uploader.Upload, Ack(transferId, nr))
                    // timeout
                    send(entityId, DownloadMaxTime, Downloader.Download, DownloadTimeout(() => {
                        waitingForFinish.get(transferId).foreach(transfer => {
                            log("timed out transfer after last packet " + transferId)
                            transfer.onFinish(false)
                            waitingForFinish -= transferId
                        })
                    }))
                })
        }
    }

    private def scheduleTimeout(transferId: String, packet: Int) =
        send(entityId, DownloadMaxTime, Downloader.Download, DownloadTimeout(() => {
            // if the download is still active and if the current packet
            // is equal to the timeout packet kill the transfer, otherwise
            // continue.
            downloads.get(transferId).foreach(transfer =>
                if (transfer.packetNumber == packet) {
                    log("timed out download " + transferId + " at packet " + packet + " of " + transfer.packetCount)
                    transfer.onFinish(false)
                    downloads -= transferId
                })
        }))

}

private[processing] abstract sealed class DownloadMessage
case class DownloadTimeout(run: () => Unit) extends DownloadMessage
case class FinishDownload(transferId: String) extends DownloadMessage
case class Packet(transferId: String, number: Int, size: Double) extends DownloadMessage
