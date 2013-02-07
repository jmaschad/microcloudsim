package de.jmaschad.storagesim.model.processing

import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.CloudSim
import scala.util.Random

object Downloader {
    private val Base = 10400
    val Download = Base + 1
}
import Downloader._

class Downloader(
    send: (Int, Double, Int, Object) => Unit,
    log: String => Unit) {

    var downloads = Map.empty[String, Transfer]

    def start(transferId: String, size: Double, source: Int, process: (Double, () => Unit) => Unit, onFinish: Boolean => Unit) = {
        log("adding download " + transferId)
        downloads += transferId -> new Transfer(source, Transfer.packetSize(size), Transfer.packetCount(size), process, onFinish)
    }

    def process(source: Int, request: Object) = request match {
        case Packet(transferId, nr, size) =>
            packetReceived(transferId, nr, size)

        case TimeoutDownlad(transferId) =>
            downloads(transferId).onFinish(false)
            downloads -= transferId

        case _ => throw new IllegalStateException("request error")
    }

    def killed() = {
        // send reset to avoid necessity of timeout handling 
        downloads.foreach(d => send(d._2.partner, 0.0, Uploader.Upload, TimeoutUpload(d._1)))

        // don't call the downloads onFinish, we were killed!
        downloads = Map.empty[String, Transfer]
    }

    private def packetReceived(transferId: String, nr: Int, size: Double) = {
        val tracker = downloads(transferId)
        assert(nr == tracker.packetNumber)

        tracker.process(size, () => {
            send(tracker.partner, 0.0, Uploader.Upload, Ack(transferId, nr))
        })
        tracker.nextPacket() match {
            case Some(newTracker) =>
                downloads += transferId -> newTracker
            case None =>
                log("download from " + CloudSim.getEntityName(tracker.partner) + " completed")
                downloads -= transferId
        }
    }
}

private[processing] abstract sealed class DownloadMessage
case class TimeoutDownlad(transferId: String) extends DownloadMessage
case class Packet(transferId: String, number: Int, size: Double) extends DownloadMessage
