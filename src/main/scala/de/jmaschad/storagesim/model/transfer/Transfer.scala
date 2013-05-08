package de.jmaschad.storagesim.model.transfer

import de.jmaschad.storagesim.Units

object Transfer {
    val MaxPacketSize = 10 * Units.KByte

    def packetCount(byteSize: Double) = (byteSize / MaxPacketSize).ceil.intValue
    def packetSize(byteSize: Double) = byteSize / packetCount(byteSize)
}