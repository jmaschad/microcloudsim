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

    dialog.messageHandler = {
        case Packet(size, timeSend) =>
            val latency = CloudSim.clock() - timeSend
            remainingPackets -= 1
            process(size, () => dialog.say(Ack, timeoutHandler))

        case FinishDownload =>
            assert(remainingPackets == 0)
            onFinish(true)

        case _ => throw new IllegalStateException("request error")
    }

    val timeoutHandler = () => onFinish(false)

    TransferProbe.add(dialog.id, size)
    dialog.say(DownloadReady, timeoutHandler)
}