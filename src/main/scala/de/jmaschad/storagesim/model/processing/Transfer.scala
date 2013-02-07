package de.jmaschad.storagesim.model.processing

import de.jmaschad.storagesim.Units

object Transfer {
    val MaxPacketSize = 1.5 * Units.KByte
    def packetCount(byteSize: Double) = (byteSize / MaxPacketSize).ceil.intValue
    def packetSize(byteSize: Double) = byteSize / packetCount(byteSize)
}

private[processing] class Transfer(
    val partner: Int,
    val packetSize: Double,
    val packetCount: Int,
    val process: (Double, () => Unit) => Unit,
    val onFinish: Boolean => Unit,
    val packetNumber: Int = 0) {

    def nextPacket(): Option[Transfer] = packetNumber < packetCount match {
        case true =>
            Some(new Transfer(partner, packetSize, packetCount, process, onFinish, packetNumber + 1))

        case false =>
            onFinish(true)
            None
    }
}