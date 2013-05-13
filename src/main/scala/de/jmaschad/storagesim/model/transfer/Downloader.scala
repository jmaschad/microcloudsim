package de.jmaschad.storagesim.model.transfer

import de.jmaschad.storagesim.model.transfer.dialogs.FinishDownload
import de.jmaschad.storagesim.model.transfer.dialogs.Packet
import de.jmaschad.storagesim.model.transfer.dialogs.DownloadReady
import de.jmaschad.storagesim.model.transfer.dialogs.Ack
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.NetworkDelay
import de.jmaschad.storagesim.model.Dialog
import de.jmaschad.storagesim.model.StorageObject

class Downloader(
    log: String => Unit,
    dialog: Dialog,
    obj: StorageObject,
    process: (StorageObject, Double, () => Unit) => Unit,
    onFinish: Boolean => Unit) {

    private val packetSize = Transfer.packetSize(obj.size)
    private var remainingPackets = Transfer.packetCount(obj.size)

    dialog.messageHandler = {
        case Packet(size, timeSend) =>
            val latency = CloudSim.clock() - timeSend
            remainingPackets -= 1
            process(obj, packetSize, () => {
                dialog.say(Ack, timeoutHandler)
            })

        case FinishDownload =>
            assert(remainingPackets == 0)
            onFinish(true)

        case _ => throw new IllegalStateException("request error")
    }

    val timeoutHandler = () => onFinish(false)

    dialog.say(DownloadReady, timeoutHandler)
}