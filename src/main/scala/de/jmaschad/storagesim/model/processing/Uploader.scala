package de.jmaschad.storagesim.model.processing

import Uploader._
import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.CloudSim

object Uploader {
    private val Base = 10450
    val Upload = Base + 1
}

class Uploader(
    send: (Int, Double, Int, Object) => Unit,
    log: String => Unit) {

    private var uploads = Map.empty[String, Transfer]
    var partnerFinished = Map.empty[String, Int]

    def start(transferId: String, size: Double, target: Int, processing: (Double, () => Unit) => Unit, onFinish: Boolean => Unit) = {
        log("adding upload " + transferId)
        uploads += transferId -> new Transfer(target, Transfer.packetSize(size), Transfer.packetCount(size), processing, onFinish)

        // introduce a small delay, this allows the uploader to send some message before the transfer starts
        sendNextPacket(transferId, 0.01)
    }

    def process(source: Int, request: Object) = request match {
        case Ack(transferId, nr) =>
            ifPartnerFinishedUploadNextPacket(transferId, nr)

        case TimeoutUpload(transferId) =>
            log("TO upload: " + transferId)
            uploads(transferId).onFinish(false)
            uploads -= transferId

        case _ => throw new IllegalStateException("request error")
    }

    def killed() = {
        // send reset to avoid necessity of timeout handling 
        uploads.foreach(upload => send(upload._2.partner, 0.0, Downloader.Download, TimeoutDownlad(upload._1)))

        // don't call the downloads onFinish, we were killed!
        uploads = Map.empty[String, Transfer]
        partnerFinished = Map.empty[String, Int]
    }

    private def ifPartnerFinishedUploadNextPacket(transferId: String, packetNumber: Int) = {
        // packet was send and acked
        if (partnerFinished.getOrElse(transferId, -1) == packetNumber) {
            val tracker = uploads(transferId)
            tracker.nextPacket() match {
                case Some(newTracker) =>
                    uploads += transferId -> newTracker
                    sendNextPacket(transferId)
                case None =>
                    log("upload to " + CloudSim.getEntityName(tracker.partner) + " completed")
                    send(tracker.partner, 0.0, Downloader.Download, FinishDownload(transferId))
                    tracker.onFinish(true)
                    uploads -= transferId
                    partnerFinished -= transferId
            }

        } else {
            partnerFinished += (transferId -> packetNumber)
        }
    }

    private def sendNextPacket(transferId: String, delay: Double = 0.0): Unit = {
        val upload = uploads(transferId)
        send(upload.partner, delay, Downloader.Download, Packet(transferId, upload.packetNumber, upload.packetSize))

        upload.process(upload.packetSize, () => {
            ifPartnerFinishedUploadNextPacket(transferId, upload.packetNumber)
        })
    }
}

private[processing] abstract sealed class UploadMessage
case class TimeoutUpload(transferId: String) extends UploadMessage
case class Ack(transferId: String, number: Int) extends UploadMessage