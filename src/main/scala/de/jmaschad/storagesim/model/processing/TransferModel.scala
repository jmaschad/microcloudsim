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

    var transfers = Map.empty[Transfer, TransferTracker]
    var partnerFinished = Map.empty[Transfer, Int]
    var transactions = Map.empty[StorageObject, StorageTransaction]

    def tick() = {
        transfers = transfers.map(t => t._1 -> t._2.countDown).collect({ case (transfer, Some(tracker)) => transfer -> tracker })
    }

    def startUpload(loadTransaction: LoadTransaction, target: Int, onFinish: Boolean => Unit) = {
        val storageObject = loadTransaction.storageObject
        val upload = new Transfer(storageObject, packetCount(storageObject.size))
        transactions += (storageObject -> loadTransaction)
        transfers += upload -> new TransferTracker(target, onFinish)

        send(target, Transfer, Start(upload))
    }

    def expectDownload(storeTransaction: StoreTransaction) = {
        transactions += storeTransaction.storageObject -> storeTransaction
    }

    def process(source: Int, request: Object) = request match {
        case Ack(Start(upload)) =>
            transfers(upload).resetTimeout
            uploadNextPacket(upload)

        case Ack(Packet(upload, packetNumber, _)) =>
            transfers(upload).resetTimeout
            ifPartnerFinishedUploadNextPacket(upload, packetNumber)

        case start @ Start(upload) =>
            uploadRequested(upload, source)
            send(source, Transfer, Ack(start))

        case packet @ Packet(_, _, _) =>
            packetReceived(packet)

        case _ => throw new IllegalStateException("request error")
    }

    def reset() = {
        transfers = Map.empty[Transfer, TransferTracker]
        partnerFinished = Map.empty[Transfer, Int]
        transactions = Map.empty[StorageObject, StorageTransaction]
    }

    private def uploadRequested(upload: Transfer, source: Int) = {
        assert(transactions.contains(upload.storageObject))
        transfers += (upload -> new TransferTracker(source, _ => {}))
    }

    private def packetReceived(packet: Packet) = {
        val transfer = packet.transfer
        assert(transfers.contains(transfer))
        assert(transactions.contains(transfer.storageObject))

        val tracker = transfers(transfer)
        assert(packet.number == tracker.packet)

        tracker.nextPacket() match {
            case Some(newTracker) =>
                processing.addObjectDownload(packet.size, transactions(transfer.storageObject), () => {
                    send(newTracker.partner, Transfer, Ack(packet))
                })
                transfers += transfer -> newTracker
            case None =>
                transfers -= transfer
        }
    }

    private def ifPartnerFinishedUploadNextPacket(transfer: Transfer, packetNumber: Int) = {
        assert(transfers.contains(transfer))
        assert(transactions.contains(transfer.storageObject))

        if (partnerFinished.getOrElse(transfer, -1) == packetNumber) {
            uploadNextPacket(transfer)
        } else {
            partnerFinished += (transfer -> packetNumber)
        }
    }

    private def uploadNextPacket(transfer: Transfer): Unit = {
        assert(transfers.contains(transfer))
        assert(transactions.contains(transfer.storageObject))

        transfers(transfer).nextPacket match {
            case Some(tracker) =>
                send(tracker.partner, Transfer, Packet(transfer, tracker.packet, packetSize(transfer.storageObject.size)))
                processing.addObjectUpload(transfer.storageObject, transactions(transfer.storageObject), () => {
                    ifPartnerFinishedUploadNextPacket(transfer, tracker.packet)
                })
            case None =>
                transfers -= transfer
        }

    }

    private def packetCount(byteSize: Double) = (byteSize / MaxPacketSize).ceil.intValue
    private def packetSize(byteSize: Double) = byteSize / packetCount(byteSize)
}

private[processing] class TransferTracker(
    val partner: Int,
    onFinish: Boolean => Unit,
    countDown: Int = TransferModel.MaxPacketTicks,
    val packet: Int = 0) {

    def resetTimeout(): TransferTracker = new TransferTracker(partner, onFinish, TransferModel.MaxPacketTicks)

    def countDown(): Option[TransferTracker] = countDown > 0 match {
        case true =>
            Some(new TransferTracker(partner, onFinish, countDown - 1, packet))
        case false =>
            onFinish(false)
            None
    }

    def nextPacket(): Option[TransferTracker] = packet < TransferModel.MaxPacketTicks match {
        case true =>
            Some(new TransferTracker(partner, onFinish, countDown, packet + 1))

        case false =>
            onFinish(true)
            None
    }
}

class Transfer(val storageObject: StorageObject, val packetCount: Int)

private[processing] abstract sealed class Request
case class Ack(request: Request) extends Request
case class Start(transfer: Transfer) extends Request
case class Packet(transfer: Transfer, number: Int, size: Double) extends Request
