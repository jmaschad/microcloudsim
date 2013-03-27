package de.jmaschad.storagesim.model.transfer

import de.jmaschad.storagesim.model.transfer.dialogs.FinishDownload
import de.jmaschad.storagesim.model.transfer.dialogs.Packet
import de.jmaschad.storagesim.model.transfer.dialogs.Ack
import org.cloudbus.cloudsim.core.CloudSim

class Upload(
    log: String => Unit,
    dialog: Dialog,
    size: Double,
    process: (Double, () => Unit) => Unit,
    onFinish: Boolean => Unit) {

    private val packetSize = Transfer.packetSize(size)
    private var remainingPackets = Transfer.packetCount(size)

    private var ackReceived = false
    private var processingFinished = false
    private var isCanceled = false

    dialog.messageHandler = processMessage _
    val timeoutHandler = () => onFinish(false)

    TransferProbe.add(dialog.id, size)
    sendNextPacket

    def cancel(transferId: String) =
        cancelAfterProcessing(transferId)

    private def processMessage(content: AnyRef) = content match {
        case Ack =>
            ackReceived = true
            synchronizeAndContinue()

        case _ =>
            throw new IllegalStateException
    }

    private def synchronizeAndContinue() = {
        // partner timed out
        if (isCanceled) {
            onFinish(false)
        }

        // packet was send and acked
        if (ackReceived && processingFinished) {
            remainingPackets -= 1
            if (remainingPackets > 0) {
                sendNextPacket()
            } else {
                dialog.say(FinishDownload, timeoutHandler)
                onFinish(true)
            }
        }
    }

    private def cancelAfterProcessing(transferId: String) =
        if (!processingFinished) {
            isCanceled = true
        } else {
            log("upload canceled " + TransferProbe.finish(dialog.id))
            onFinish(false)
        }

    private def sendNextPacket(): Unit = {
        dialog.say(Packet(packetSize, CloudSim.clock), timeoutHandler)
        ackReceived = false

        processingFinished = false
        process(packetSize, () => {
            processingFinished = true
            synchronizeAndContinue
        })
    }
}