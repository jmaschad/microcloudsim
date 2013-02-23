package de.jmaschad.storagesim.model.transfer

class Download(
    log: String => Unit,
    dialog: Dialog,
    size: Double,
    process: (Double, () => Unit) => Unit,
    onFinish: Boolean => Unit) {

    val packetSize = Transfer.packetSize(size)
    var remainingPackets = Transfer.packetCount(size)

    TransferProbe.add(dialog.id, size)
    dialog.messageHandler = processMessage _

    val timeoutHandler = () => {
        onFinish(false)
    }
    dialog.say(DownloadReady, timeoutHandler)

    private def processMessage(content: AnyRef) = content match {
        case Packet(size) =>
            packetReceived(size)

        case FinishDownload =>
            assert(remainingPackets == 0)
            onFinish(true)

        case _ => throw new IllegalStateException("request error")
    }

    private def packetReceived(size: Double) = {
        remainingPackets -= 1
        process(size, () => dialog.say(Ack, timeoutHandler))
    }
}

private[transfer] abstract sealed class DownloadMessage
case class FinishDownload extends DownloadMessage
case class Packet(size: Double) extends DownloadMessage
