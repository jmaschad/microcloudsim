package de.jmaschad.storagesim.model.transfer

import de.jmaschad.storagesim.model.transfer.dialogs.FinishDownload
import de.jmaschad.storagesim.model.transfer.dialogs.Packet
import de.jmaschad.storagesim.model.transfer.dialogs.DownloadReady
import de.jmaschad.storagesim.model.transfer.dialogs.Ack
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.NetworkDelay

class Download(
    log: String => Unit,
    dialog: Dialog,
    size: Double,
    process: (Double, () => Unit) => Unit,
    onFinish: Boolean => Unit) {

    private val packetSize = Transfer.packetSize(size)
    private var remainingPackets = Transfer.packetCount(size)

    TransferProbe.add(dialog.id, size)
    dialog.messageHandler = processMessage _

    val timeoutHandler = () => {
        onFinish(false)
    }
    dialog.say(DownloadReady, timeoutHandler)

    private def processMessage(content: AnyRef) = content match {
        case Packet(size, timeSend) =>
            val latency = CloudSim.clock() - timeSend
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