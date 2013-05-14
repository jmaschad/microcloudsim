package de.jmaschad.storagesim

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import scala.collection.immutable.Queue
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.ProcessingModel
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.descriptive.SummaryStatistics

object StatsCentral extends SimEntity("StatsCentral") {
    private val StatsInterval = 1
    private val StatsEvent = 18001

    def wakeup() = {
        Log.line("Stats Central", "BIN DA!")
    }

    override def startEntity() = {
        scheduleStatsLog()
    }

    override def shutdownEntity() = {}
    override def processEvent(event: SimEvent) = event.getTag match {
        case StatsEvent =>
            logStats()
            scheduleStatsLog()

        case _ =>
            throw new IllegalStateException
    }

    private def scheduleStatsLog() = {
        schedule(getId, StatsInterval, StatsEvent)
    }

    private def logStats() = {
        var message = requestStats + " " + loadStats + " " + repairStats
        if (!message.trim().isEmpty())
            Log.line("SC", message)
    }

    val latencyStats = new SummaryStatistics()
    val distanceStats = new SummaryStatistics()
    var requestCount = BigDecimal(0.0)

    def requestCompleted(meanLatency: Double, distance: Double) = {
        latencyStats.addValue(meanLatency)
        distanceStats.addValue(distance)
        requestCount += 1.0
    }

    private def requestStats: String = {
        "%2.2freq/s %.3fms %.2fdist".format(requestCount / CloudSim.clock(), latencyStats.getMean(), distanceStats.getMean())
    }

    /*
     * Server load stats over 1 second: {median, min, max} data amount to transfer
     */
    private def loadStats: String = {
        var stats = Set.empty[String]

        val ulStats = new DescriptiveStatistics(ProcessingModel.allLoadUp().toArray)
        if (ulStats.getSum > 0.0)
            stats += "UL[MEAN %.2fMbit/s STD.DEV %.2fMbit/s]".
                format(ulStats.getMean * 8.0, ulStats.getStandardDeviation * 8.0)

        val dlStats = new DescriptiveStatistics(ProcessingModel.allLoadDown().toArray)
        if (dlStats.getSum > 0.0)
            stats += "DL[MEAN %.2fMbit/s STD.DEV %.2fMbit/s]".
                format(dlStats.getMean * 8.0, dlStats.getStandardDeviation * 8.0)

        stats.mkString(" ")
    }

    /*
     * Repair stats: mean repair bandwidth over 10 sec and over all, repair time, repair size
     */
    case class RepairInfo(clock: Double, size: Double)
    private var repairInfos = Queue.empty[RepairInfo]
    private var startOfRepair = Double.NaN
    private var totalSizeOfRepair = Double.NaN
    private var finishedAmount = 0.0

    def startRepair(totalSize: Double) =
        if (startOfRepair.isNaN) {
            startOfRepair = CloudSim.clock()
            totalSizeOfRepair = totalSize
            Log.line("SC", "Starting repair of %.3f GB".format(totalSize / 1024))
        } else {
            totalSizeOfRepair += totalSize
            Log.line("SC", "Adding repair of %.3fGB".format(totalSize / 1024))
        }

    def finishRepair() = {
        val totalTime = CloudSim.clock() - startOfRepair
        val totalMeanBW = totalSizeOfRepair / totalTime
        Log.line("SC", "Finished repair of %.3fMB in %.3fs @ %.3fMbit/s".format(totalSizeOfRepair, totalTime, totalMeanBW * 8))
        startOfRepair = Double.NaN
        totalSizeOfRepair = Double.NaN
        finishedAmount = 0.0

        // replaced real error model with one repair
        CloudSim.terminateSimulation(CloudSim.clock() + 120)
    }

    def progressRepair(size: Double) = {
        finishedAmount += size
    }

    private def repairStats: String =
        if (!startOfRepair.isNaN) {
            val remain = totalSizeOfRepair - finishedAmount
            val bwTotal = finishedAmount / (CloudSim.clock() - startOfRepair)
            "REP[%.2fGB %.3fMbit/s]".format(remain / 1024, bwTotal * 8)
        } else {
            ""
        }
}