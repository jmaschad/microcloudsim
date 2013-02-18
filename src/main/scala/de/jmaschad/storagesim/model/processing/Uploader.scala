package de.jmaschad.storagesim.model.processing

import Uploader._
import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.microcloud.MicroCloud

object Uploader {
    val UploadMaxTime = 2.0

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
        assert(target != entityId)

        TransferProbe.add(transferId, size)
        uploads += transferId -> new Transfer(target, Transfer.packetSize(size), Transfer.packetCount(size), processing, onFinish)
        sendNextPacket(transferId)
    }

    def cancel(transferId: String) = {
        cancelAfterProcessing(transferId)
    }

    def process(source: Int, request: Object) = request match {
        case Ack(transferId, nr) =>
            ifPartnerFinishedUploadNextPacket(transferId, nr)

        case UploadTimeout(run) => run

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

    def reset(): Uploader = new Uploader(send, log, entityId)

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
                    send(tracker.partner, 0.0, Downloader.Download, FinishDownload(transferId))
                    tracker.onFinish(true)
                    uploads -= transferId
                    partnerFinished -= transferId
            }

        } else {
            partnerFinished += (transferId -> packetNumber)
        }
    }

    private def sendNextPacket(transferId: String): Unit = {
        val upload = uploads(transferId)
        send(upload.partner, 0.0, Downloader.Download, Packet(transferId, upload.packetNumber, upload.packetSize))

        isProcessing += transferId
        upload.process(upload.packetSize, () => {
            isProcessing -= transferId
            scheduleTimeout(transferId, upload.packetNumber)
            ifPartnerFinishedUploadNextPacket(transferId, upload.packetNumber)
        })
    }

    private def scheduleTimeout(transferId: String, packet: Int) = {
        send(entityId, UploadMaxTime, Uploader.Upload, UploadTimeout(() => {
            uploads.get(transferId).foreach(transfer =>
                if (transfer.packetNumber == packet) {
                    log("timed out upload " + transferId)
                    cancelAfterProcessing(transferId)
                })
        }))
    }
}

private[processing] abstract sealed class UploadMessage
case class UploadTimeout(run: () => Unit) extends UploadMessage
case class Ack(transferId: String, number: Int) extends UploadMessage