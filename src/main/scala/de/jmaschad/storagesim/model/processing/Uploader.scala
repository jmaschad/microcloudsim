package de.jmaschad.storagesim.model.processing

import Uploader._
import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.microcloud.MicroCloud

object Uploader {
    private val Base = 10450
    val Upload = Base + 1
}

class Uploader(
    send: (Int, Double, Int, Object) => Unit,
    log: String => Unit,
    entityId: Int) {

    private var uploads = Map.empty[String, Transfer]
    var partnerFinished = Map.empty[String, Int]

    var canceled = Set.empty[String]
    var isProcessing = Set.empty[String]

    def start(transferId: String, size: Double, target: Int, processing: (Double, () => Unit) => Unit, onFinish: Boolean => Unit) = {
        log("adding upload " + transferId)
        if (target == entityId) {
            println
        }
        uploads += transferId -> new Transfer(target, Transfer.packetSize(size), Transfer.packetCount(size), processing, onFinish)
        TransferProbe.add(transferId, size)
        // introduce a small delay, this allows the uploader to send some message before the transfer starts
        sendNextPacket(transferId, 0.01)
    }

    def cancel(transferId: String) = {
        val transfer = uploads(transferId)
        send(transfer.partner, 0.0, MicroCloud.InterCloudRequest, TimeoutDownlad(transferId))
        cancelAfterProcessing(transferId)
    }

    def process(source: Int, request: Object) = request match {
        case Ack(transferId, nr) =>
            ifPartnerFinishedUploadNextPacket(transferId, nr)

        case TimeoutUpload(transferId) =>
            log("Time out upload " + transferId + " " + TransferProbe.finish(transferId))
            cancelAfterProcessing(transferId)

        case _ => throw new IllegalStateException("request error")
    }

    private def cancelAfterProcessing(transferId: String) =
        // uploader is still processing and did not call ifPartnerFinishedUploadNextPacket
        if (isProcessing.contains(transferId)) {
            canceled += transferId
        } // uploader finished processing and is currently waiting
        else {
            uploads(transferId).onFinish(false)
            uploads -= transferId
            partnerFinished -= transferId
        }

    def reset(): Uploader = {
        // send reset to avoid necessity of timeout handling 
        uploads.foreach(upload => send(upload._2.partner, 0.0, Downloader.Download, TimeoutDownlad(upload._1)))

        // don't call the downloads onFinish, we were killed!
        new Uploader(send, log, entityId)
    }

    private def ifPartnerFinishedUploadNextPacket(transferId: String, packetNumber: Int) = {
        // partner timed out
        if (canceled.contains(transferId)) {
            uploads(transferId).onFinish(false)

            uploads -= transferId
            partnerFinished -= transferId
            canceled -= transferId
        }

        // packet was send and acked
        if (partnerFinished.getOrElse(transferId, -1) == packetNumber) {
            val tracker = uploads(transferId)
            tracker.nextPacket() match {
                case Some(newTracker) =>
                    uploads += transferId -> newTracker
                    sendNextPacket(transferId)
                case None =>
                    log("upload to " + CloudSim.getEntityName(tracker.partner) + " completed " +
                        TransferProbe.finish(transferId))
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

        isProcessing += transferId
        upload.process(upload.packetSize, () => {
            isProcessing -= transferId
            ifPartnerFinishedUploadNextPacket(transferId, upload.packetNumber)
        })
    }
}

private[processing] abstract sealed class UploadMessage
case class TimeoutUpload(transferId: String) extends UploadMessage
case class Ack(transferId: String, number: Int) extends UploadMessage