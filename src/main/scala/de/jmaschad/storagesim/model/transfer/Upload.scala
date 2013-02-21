package de.jmaschad.storagesim.model.transfer

class Upload(
    log: String => Unit,
    dialog: Dialog,
    size: Double,
    process: (Double, () => Unit) => Unit,
    onFinish: Boolean => Unit) {

    val packetSize = Transfer.packetSize(size)
    var remainingPackets = Transfer.packetCount(size)

    var ackReceived = false
    var processingFinished = false
    var isCanceled = false

    dialog.messageHandler = processMessage _
    val timeoutHandler = () => {
        log("upload timed out " + TransferProbe.finish(dialog.id))
        onFinish(false)
    }

    TransferProbe.add(dialog.id, size)
    sendNextPacket

    def cancel(transferId: String) = {
        cancelAfterProcessing(transferId)
    }

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
                log("upload finished " + TransferProbe.finish(dialog.id))
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
        dialog.say(Packet(packetSize), timeoutHandler)
        ackReceived = false

        processingFinished = false
        process(packetSize, () => {
            processingFinished = true
            synchronizeAndContinue
        })
    }
}

private[transfer] abstract sealed class UploadMessage
case class DownloadReady extends UploadMessage
case class Ack extends UploadMessage