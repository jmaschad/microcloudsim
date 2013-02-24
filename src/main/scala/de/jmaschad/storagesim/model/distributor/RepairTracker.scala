package de.jmaschad.storagesim.model.distributor

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.processing.StorageObject

case class DownloadRequest(cloud: Int, obj: StorageObject)

class RepairTracker(
    val log: String => Unit,
    private var downloads: Set[DownloadRequest]) {
    val totalTransferSize = downloads.map(_.obj.size).sum
    val startOfRepair = CloudSim.clock

    log("starting repair [%.2fMB]".format(totalTransferSize))

    def complete(download: DownloadRequest) = {
        downloads -= download
        if (downloads.isEmpty) {
            logSummary()
        }
    }

    def isDone: Boolean = downloads.isEmpty

    def removeCloud(cloud: Int) =
        if (downloads.count(_.cloud == cloud) > 0) throw new IllegalStateException

    private def logSummary() = {
        val repairTime = CloudSim.clock - startOfRepair
        val averageBandwidth = (totalTransferSize * 8) / repairTime
        log("finished repair in %.3fs with avg. %.3fMbit/s".format(repairTime, averageBandwidth))
    }
}