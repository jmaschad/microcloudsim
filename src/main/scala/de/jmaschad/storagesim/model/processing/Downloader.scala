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

    def start(transferId: String, size: Double, source: Int, process: (Double, () => Unit) => Unit, onFinish: Boolean => Unit) = {
        assert(source != entityId)

        log("adding download " + transferId)
        val transfer = new Transfer(source, Transfer.packetSize(size), Transfer.packetCount(size), process, onFinish)
        downloads += transferId -> transfer
        scheduleTimeout(transferId, transfer.packetNumber)

        TransferProbe.add(transferId, size)
    }

    private def scheduleTimeout(transferId: String, packet: Int) =
        send(entityId, DownloadMaxTime, Downloader.Download, DownloadTimeout(() => {
            // if the download is still active and if the current packet
            // is equal to the timeout packet kill the transfer, otherwise
            // continue.
            downloads.get(transferId).foreach(transfer =>
                if (transfer.packetNumber == packet) {
                    log("timed out download " + transferId)
                    transfer.onFinish(false)
                    downloads -= transferId
                })
        }))

    def process(source: Int, request: Object) = request match {
        case Packet(transferId, nr, size) =>
            packetReceived(transferId, nr, size)

        case FinishDownload(transferId) =>
            log("download from " + CloudSim.getEntityName(source) +
                " completed. " + TransferProbe.finish(transferId))
            downloads(transferId).onFinish(true)
            downloads -= transferId

        case DownloadTimeout(run) => run()

        case _ => throw new IllegalStateException("request error")
    }

    def reset(): Downloader =
        new Downloader(send, log, entityId)

    private def packetReceived(transferId: String, nr: Int, size: Double) = {
        val tracker = downloads(transferId)
        assert(nr == tracker.packetNumber)

        tracker.process(size, () => {
            // Inform the uploader, that the packet was received 
            send(tracker.partner, 0.0, Uploader.Upload, Ack(transferId, nr))

            tracker.nextPacket() match {
                case Some(newTracker) =>
                    downloads += transferId -> newTracker
                    scheduleTimeout(transferId, newTracker.packetNumber)

                case None =>
                    send(entityId, DownloadMaxTime, Downloader.Download, DownloadTimeout(() => {
                        // If FinishDownload was not received, time out. This
                        // can happen if the Downloader is finished before the Uploader
                        // and the uploader crashes while processing the last packet
                        // of the transfer
                        downloads.get(transferId).foreach(transfer => {
                            log("timed out transfer " + transferId)
                            transfer.onFinish(false)
                            downloads -= transferId
                        })
                    }))
            }
        })

    }
}

private[processing] abstract sealed class DownloadMessage
case class DownloadTimeout(run: () => Unit) extends DownloadMessage
case class FinishDownload(transferId: String) extends DownloadMessage
case class Packet(transferId: String, number: Int, size: Double) extends DownloadMessage
