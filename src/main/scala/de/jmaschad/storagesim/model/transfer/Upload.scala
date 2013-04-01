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

    TransferProbe.add(dialog.id, size)
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
        process(packetSize, () => {
            send = true
            synchronizeAndContinue
        })
    }
}