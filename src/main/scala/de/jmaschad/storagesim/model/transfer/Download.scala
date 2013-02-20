package de.jmaschad.storagesim.model.transfer

class Download(
    dialog: Dialog,
    size: Double,
    process: (Double, () => Unit) => Unit,
    onFinish: Boolean => Unit) {

    val packetSize = Transfer.packetSize(size)
    var remainingPackets = Transfer.packetCount(size)

    TransferProbe.add(dialog.dialogId, size)
    dialog.process = processMessage _
    dialog.onTimeout = () => { onFinish(false) }

    assert(dialog.canSay)
    dialog.say(DownloadReady)

    private def processMessage(message: AnyRef) = message match {
        case Packet(size) =>
            packetReceived(size)

        case FinishDownload =>
            assert(remainingPackets == 0)
            onFinish(true)

        case _ => throw new IllegalStateException("request error")
    }

    private def packetReceived(size: Double) = {
        remainingPackets -= 1
        process(size, () => dialog.say(Ack))
    }
}

private[transfer] abstract sealed class DownloadMessage
case class FinishDownload extends DownloadMessage
case class Packet(size: Double) extends DownloadMessage
