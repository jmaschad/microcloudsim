package de.jmaschad.storagesim.model.transfer

import org.cloudbus.cloudsim.core.CloudSim

object TransferProbe {
    private class Entry(val size: Double, val start: Double)
    private var transfers = Map.empty[String, Entry]

    def add(transferId: String, size: Double) = {
        transfers += transferId -> new Entry(size, CloudSim.clock())
    }

    def finish(transferId: String): String = {
        val entry = transfers(transferId)
        val time = CloudSim.clock() - entry.start
        val bw = (entry.size / time) * 8

        // bandwidth in Mbit/s
        "[%.3f seconds, avg. %.3f Mbit/s]".format(time, bw)
    }
}