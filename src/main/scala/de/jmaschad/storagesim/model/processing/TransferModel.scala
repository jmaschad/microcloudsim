package de.jmaschad.storagesim.model.processing

import de.jmaschad.storagesim.Units
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.CloudSim
import scala.util.Random
import de.jmaschad.storagesim.util.Ticker

object TransferModel {
    val MaxPacketSize = 1.5 * Units.KByte // approximation of maximum TCP payload
    val TickDelay = 1
    val MaxPacketTicks = 2

    private val Base = 10400
    val Transfer = Base + 1

    def transferId(): String = CloudSim.clock() + "-" + Random.nextLong
}
import TransferModel._

class TransferModel(
    send: (Int, Double, Int, Object) => Unit,
    log: String => Unit,
    source: SimEntity,
    processing: ProcessingModel) {

    var transfers = Map.empty[String, TransferTracker]
    var partnerFinished = Map.empty[String, Int]

    var expectedDownloadTransaction = Map.empty[StorageObject, StorageTransaction]
    var expectedDownloadTimeout = Map.empty[StorageTransaction, Int]

    def upload(transferId: String, size: Double, target: Int, process: (Double, () => Unit) => Unit, onFinish: Boolean => Unit) = {
        addTransfer(transferId, new TransferTracker(target, packetSize(size), packetCount(size), process, onFinish))

        // introduce a small delay, this allows the uploader to send some message before the transfer starts
        uploadNextPacket(transferId, 0.01)
    }

    def download(transferId: String, size: Double, source: Int, process: (Double, () => Unit) => Unit, onFinish: Boolean => Unit) = {
        addTransfer(transferId, new TransferTracker(source, packetSize(size), packetCount(size), process, onFinish))
    }

    def process(source: Int, request: Object) = request match {
        case Ack(transferId, packetNumber) =>
            log("received ack " + packetNumber + " for transaction " + transferId)
            transfers(transferId).resetTimeout
            ifPartnerFinishedUploadNextPacket(transferId, packetNumber)

        case packet @ Packet(transferId, nr, size) =>
            log("received packet " + nr + " for transaction " + transferId)
            packetReceived(transferId, nr, size)

        case _ => throw new IllegalStateException("request error")
    }

    def reset() = {
        transfers = Map.empty[String, TransferTracker]
        partnerFinished = Map.empty[String, Int]
    }

    private def addTransfer(id: String, tracker: TransferTracker) = {
        val transfer = id -> tracker
        transfers += transfer

        Ticker(TickDelay, {
            tracker.countDown match {
                case Some(newTracker) =>
                    transfers += id -> newTracker
                    true
                case _ =>
                    transfers -= id
                    false
            }
        })
    }

    private def packetReceived(transferId: String, nr: Int, size: Double) = {
        val tracker = transfers(transferId)
        assert(nr == tracker.packetNumber)
        tracker.process(size, () => {
            send(tracker.partner, 0.0, Transfer, Ack(transferId, nr))
        })

        tracker.nextPacket() match {
            case Some(newTracker) =>
                transfers += transferId -> newTracker
            case None =>
                log("download from " + CloudSim.getEntityName(tracker.partner) + " completed")
                transfers -= transferId
        }
    }

    private def uploadNextPacket(transferId: String, delay: Double = 0.0): Unit = {
        val tracker = transfers(transferId)
        send(tracker.partner, delay, Transfer, Packet(transferId, tracker.packetNumber, tracker.packetSize))
        tracker.process(tracker.packetSize, () => {
            ifPartnerFinishedUploadNextPacket(transferId, tracker.packetNumber)
        })
    }

    private def ifPartnerFinishedUploadNextPacket(transferId: String, packetNumber: Int) = {
        // packet was send and acked
        if (partnerFinished.getOrElse(transferId, -1) == packetNumber) {
            val tracker = transfers(transferId)
            tracker.nextPacket() match {
                case Some(newTracker) =>
                    transfers += transferId -> newTracker
                    uploadNextPacket(transferId)
                case None =>
                    log("upload to " + CloudSim.getEntityName(tracker.partner) + " completed")
                    transfers -= transferId
                    partnerFinished -= transferId
            }

        } else {
            partnerFinished += (transferId -> packetNumber)
        }
    }

    private def packetCount(byteSize: Double) = (byteSize / MaxPacketSize).ceil.intValue
    private def packetSize(byteSize: Double) = byteSize / packetCount(byteSize)
}

private[processing] class TransferTracker(
    val partner: Int,
    val packetSize: Double,
    val packetCount: Int,
    val process: (Double, () => Unit) => Unit,
    onFinish: Boolean => Unit,
    countDown: Int = MaxPacketTicks,
    val packetNumber: Int = 0) {

    def resetTimeout(): TransferTracker = new TransferTracker(partner, packetSize, packetCount, process, onFinish, MaxPacketTicks)

    def countDown(): Option[TransferTracker] = countDown > 0 match {
        case true =>
            Some(new TransferTracker(partner, packetSize, packetCount, process, onFinish, countDown - 1, packetNumber))
        case false =>
            onFinish(false)
            None
    }

    def nextPacket(): Option[TransferTracker] = packetNumber < packetCount match {
        case true =>
            Some(new TransferTracker(partner, packetSize, packetCount, process, onFinish, countDown, packetNumber + 1))

        case false =>
            onFinish(true)
            None
    }
}

private[processing] abstract sealed class Message
case class Ack(transferId: String, number: Int) extends Message
case class Packet(transferId: String, number: Int, size: Double) extends Message
