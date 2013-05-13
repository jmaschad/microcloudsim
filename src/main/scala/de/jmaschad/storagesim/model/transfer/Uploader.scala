package de.jmaschad.storagesim.model.transfer

import de.jmaschad.storagesim.model.transfer.dialogs.FinishDownload
import de.jmaschad.storagesim.model.transfer.dialogs.Packet
import de.jmaschad.storagesim.model.transfer.dialogs.Ack
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.Dialog
import de.jmaschad.storagesim.model.StorageObject

class Uploader(
    log: String => Unit,
    dialog: Dialog,
    obj: StorageObject,
    process: (StorageObject, Double, () => Unit) => Unit,
    onFinish: Boolean => Unit) {

    private val packetSize = Transfer.packetSize(obj.size)
    private var remainingPackets = Transfer.packetCount(obj.size)

    private var received = false
    private var send = false
    private var isCanceled = false

    dialog.messageHandler = {
        case Ack =>
            received = true
            synchronizeAndContinue()

        case _ =>
            throw new IllegalStateException
    }

    val timeoutHandler = () => onFinish(false)

    sendNextPacket

    private def synchronizeAndContinue() =
        if (send && received) {
            remainingPackets -= 1
            if (remainingPackets > 0) {
                sendNextPacket()
            } else {
                dialog.say(FinishDownload, timeoutHandler)
                onFinish(true)
            }
        }

    private def sendNextPacket(): Unit = {
        send = false
        received = false

        dialog.say(Packet(packetSize, CloudSim.clock), timeoutHandler)
        process(obj, packetSize, () => {
            send = true
            synchronizeAndContinue
        })
    }
}