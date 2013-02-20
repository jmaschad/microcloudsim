package de.jmaschad.storagesim.model.transfer

class Upload(
    dialog: Dialog,
    size: Double,
    process: (Double, () => Unit) => Unit,
    onFinish: Boolean => Unit) {

    val packetSize = Transfer.packetSize(size)
    var remainingPackets = Transfer.packetCount(size)

    var ackReceived = false
    var processingFinished = false

    var isCanceled = false

    TransferProbe.add(dialog.dialogId, size)
    dialog.process = processMessage _
    dialog.onTimeout = () => { onFinish(false) }

    assert(!dialog.canSay)

    def cancel(transferId: String) = {
        cancelAfterProcessing(transferId)
    }

    private def processMessage(something: AnyRef) =
        something match {
            case DownloadReady =>
                sendNextPacket()

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
                dialog.say(FinishDownload)
                onFinish(true)
            }
        }
    }

    private def cancelAfterProcessing(transferId: String) =
        if (!processingFinished) {
            isCanceled = true
        } else {
            onFinish(false)
        }

    private def sendNextPacket(): Unit = {
        dialog.say(Packet(packetSize))
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