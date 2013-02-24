package de.jmaschad.storagesim.model.user

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.transfer.dialogs.Get
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog
import de.jmaschad.storagesim.model.transfer.dialogs.RequestSummary._

class RequestLog(
    log: String => Unit) {
    private class Active(size: Double) {
        private val start = CloudSim.clock()
        def finish(averageLatency: Double, summary: RequestSummary): Finished = {
            val duration = CloudSim.clock() - start
            new Finished(duration, size / duration, averageLatency, summary)
        }
    }

    private class Finished(val duration: Double, val avgBandwidth: Double, val avgLatency: Double, val summary: RequestSummary) {
        override def toString = "%s in %.3fs [%.0fms latency] @ %.3fMBit/s".format(summary, duration, avgLatency * 1000, avgBandwidth * 8)
    }

    private var activeRequests = Map.empty[Long, Active]
    private var finishedRequests = Set.empty[Finished]

    def add(request: RestDialog) = request match {
        case Get(obj) =>
            assert(!activeRequests.isDefinedAt(request.id))
            activeRequests += request.id -> new Active(obj.size)

        case _ =>
            throw new IllegalStateException
    }

    def finish(request: RestDialog, averageLatency: Double, summary: RequestSummary): Unit = {
        assert(activeRequests.contains(request.id))
        val finished = activeRequests(request.id).finish(averageLatency, summary)
        log("Request: " + finished)
        finishedRequests += finished
        activeRequests -= request.id
    }

    def finish(request: RestDialog, summary: RequestSummary): Unit = finish(request, 0.0, summary)

    def summary(): String = {
        val avgBw = finishedRequests.foldLeft(0.0)((sum, entry) => sum + entry.avgBandwidth) / finishedRequests.count(_.avgBandwidth > 0.0)
        val bySummary = finishedRequests.groupBy(_.summary)

        finishedRequests.size + " finished / " + activeRequests.size + " active requests.\n" +
            "\taverage bandwidth " + (avgBw * 8).formatted("%.2f") + "Mbit/s\n" +
            "\t" + bySummary.keys.map(key => bySummary(key).size + " " + key).mkString(", ")
    }
}