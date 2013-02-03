package de.jmaschad.storagesim.model.processing

import de.jmaschad.storagesim.Units
import TransferModel._
import org.cloudbus.cloudsim.core.SimEntity

object TransferModel {
    private val MaxPacketSize = 1.5 * Units.KByte // approximation of maximum TCP payload

    private[processing] val MaxAckTicks = 2
    private[processing] val MaxPacketTicks = 2
    val TickDelay = 1.0

    private val Base = 10400
    val Tick = Base + 1
    val Transfer = Tick + 1
}

class TransferModel(
    send: (Int, Int, Object) => Unit,
    source: SimEntity,
    processing: ProcessingModel) {

    var requestedUploads = Map.empty[Transfer, Int]
    var uploads = Map.empty[Transfer, TransferTracker]
    var partnerFinished = Map.empty[Transfer, Int]

    var downloads = Map.empty[Transfer, TransferTracker]

    var storeTransactions = Map.empty[StorageObject, StoreTransaction]
    var loadTransactions = Map.empty[StorageObject, LoadTransaction]

    def tick() = {
        def timeoutRequestedTransfers() = {
            requestedUploads = requestedUploads.map(p => p._1 -> (p._2 - 1)).filter(_._2 > 0)
        }

        def timeoutUploads() = {
            uploads.values.foreach(_.tick())
            val overdue = uploads.filter(_._2.overdue).keys
            overdue.foreach(trans => loadTransactions(trans.storageObject).complete())
            uploads --= overdue
        }

        def timeoutDownloads() = {
            downloads.values.foreach(_.tick())
            val overdue = downloads.filter(_._2.overdue).keys
            overdue.foreach(trans => storeTransactions(trans.storageObject).abort())
            downloads --= overdue
        }

        timeoutRequestedTransfers()
        timeoutUploads()
        timeoutDownloads()
    }

    def startTransfer(storageObject: StorageObject, target: Int) = {
        val upload = new Transfer(storageObject, packetCount(storageObject.size))
        requestedUploads += upload -> MaxAckTicks

        send(target, Transfer, Start(upload))
    }

    def process(source: Int, request: Object) = request match {
        case Ack(Start(transfer)) =>
            uploadAccepted(source, transfer)

        case Ack(Packet(transfer, packetNumber, _)) =>
            uploads(transfer).resetTickCount
            ifPartnerFinishedUploadNextPacket(transfer, packetNumber)

        case start @ Start(objectTransfer) =>
            uploadRequested(objectTransfer, source)
            send(source, Transfer, Ack(start))

        case packet @ Packet(_, _, _) =>
            packetReceived(packet)

        case _ => throw new IllegalStateException("request error")
    }

    private def uploadAccepted(uploadTarget: Int, upload: Transfer) = {
        assert(requestedUploads.contains(upload))

        requestedUploads -= upload
        uploads += (upload -> new TransferTracker(uploadTarget))
        uploadNextPacket(upload)
    }

    private def uploadRequested(transfer: Transfer, source: Int) = {
        assert(storeTransactions.contains(transfer.storageObject))
        downloads += (transfer -> new TransferTracker(source))
    }

    private def packetReceived(packet: Packet) = {
        val transfer = packet.transfer
        assert(downloads.contains(transfer))
        assert(storeTransactions.contains(transfer.storageObject))

        val tracker = downloads(transfer)
        assert(packet.number == tracker.nextPacket)
        tracker.resetTickCount()

        processing.addObjectDownload(packet.size, storeTransactions(transfer.storageObject), () => {
            send(tracker.partner, Transfer, Ack(packet))
        })
    }

    private def ifPartnerFinishedUploadNextPacket(transfer: Transfer, packetNumber: Int) = {
        assert(uploads.contains(transfer))
        assert(loadTransactions.contains(transfer.storageObject))

        if (partnerFinished.getOrElse(transfer, -1) == packetNumber) {
            uploadNextPacket(transfer)
        } else {
            partnerFinished += (transfer -> packetNumber)
        }
    }

    private def uploadNextPacket(transfer: Transfer): Unit = {
        assert(uploads.contains(transfer))
        assert(loadTransactions.contains(transfer.storageObject))

        val tracker = uploads(transfer)
        val packetNumber = tracker.nextPacket()
        send(tracker.partner, Transfer, Packet(transfer, packetNumber, packetSize(transfer.storageObject.size)))
        processing.addObjectUpload(transfer.storageObject, loadTransactions(transfer.storageObject), () => {
            ifPartnerFinishedUploadNextPacket(transfer, packetNumber)
        })
    }

    private def packetCount(byteSize: Double) = (byteSize / MaxPacketSize).ceil.intValue
    private def packetSize(byteSize: Double) = byteSize / packetCount(byteSize)
}

private[processing] class TransferTracker(val partner: Int) {
    var packet = 0
    var ticks = TransferModel.MaxPacketTicks

    def tick() = ticks -= 1

    def resetTickCount() = ticks = TransferModel.MaxPacketTicks

    def overdue(): Boolean = ticks <= 0

    def nextPacket(): Int = {
        packet += 1
        packet
    }
}

class Transfer(val storageObject: StorageObject, val packetCount: Int)

private[processing] abstract sealed class Request
case class Ack(request: Request) extends Request
case class Start(transfer: Transfer) extends Request
case class Packet(transfer: Transfer, number: Int, size: Double) extends Request
