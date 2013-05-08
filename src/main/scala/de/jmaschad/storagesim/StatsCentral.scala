package de.jmaschad.storagesim

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import scala.collection.immutable.Queue
import org.cloudbus.cloudsim.core.CloudSim

object StatsCentral extends SimEntity("StatsCentral") {
    private val StatsInterval = 0.5
    private val StatsEvent = 18001

    override def startEntity() = {} //scheduleStatsLog()
    override def shutdownEntity() = {}
    override def processEvent(event: SimEvent) = event.getTag match {
        case StatsEvent =>
            logStats()
            scheduleStatsLog()

        case _ =>
            throw new IllegalStateException
    }

    private def scheduleStatsLog() = schedule(getId, StatsInterval, StatsEvent)

    private def logStats() = {
        var message = requestStats() + " " + loadStats() + " " + repairStats()
        if (message.nonEmpty)
            Log.line("Stats Central", message)
    }

    /*
     * Request stats over 5 seconds: mean latency, requests per second 
     */
    case class RequestInfo(time: Double, meanLatency: Double)
    var requests = Queue.empty[RequestInfo]

    def requestCompleted(meanLatency: Double) = {
        requests = requests :+ RequestInfo(CloudSim.clock(), meanLatency)
    }

    private def requestStats(): String =
        if (requests.nonEmpty) {
            val currentTime = CloudSim.clock()
            val lastPossiblTime = currentTime - 5.0
            requests = requests dropWhile { _.time < lastPossiblTime }

            val lastTime = requests.head.time
            val requestCount = requests.size

            val requestRate = requestCount / (currentTime - lastTime)
            val meanLatency = (requests map { _.meanLatency } sum) / requestCount

            "Requests: " + requestRate + " per second with " + meanLatency + "ms mean latency"
        } else {
            ""
        }

    /*
     * Server load stats over 5 seconds: {median, mean, min, max} data amount to transfer
     */
    private def loadStats(): String = ""

    /*
     * Repair stats: duration and mean repair bandwidth
     */
    private def repairStats(): String = ""
}