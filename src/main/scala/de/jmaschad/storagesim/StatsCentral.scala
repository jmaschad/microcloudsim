package de.jmaschad.storagesim

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import scala.collection.immutable.Queue
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.ProcessingModel

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
        var message = requestStats() + " " + loadStats() + " " + repairStats()
        if (!message.trim().isEmpty())
            Log.line("SC", message)
    }

    /*
     * Request stats over 5 seconds: mean latency, requests per second 
     */
    case class RequestInfo(time: Double, meanLatency: Double)
    var reqs = Queue.empty[RequestInfo]

    def requestCompleted(meanLatency: Double) = {
        reqs = reqs :+ RequestInfo(CloudSim.clock(), meanLatency)
    }

    private def requestStats(): String =
        if (reqs.nonEmpty) {
            val currentTime = CloudSim.clock()
            val lastPossibleTime = currentTime - 5.0
            reqs = reqs dropWhile { _.time < lastPossibleTime }

            val lastTime = reqs.head.time
            val requestCount = reqs.size

            val requestRate = requestCount / (currentTime - lastTime)
            val meanLatency = (reqs map { _.meanLatency } sum) / requestCount

            "%2.2freq/s %.3fms".format(requestRate, meanLatency)
        } else {
            ""
        }

    /*
     * Server load stats over 1 second: {median, min, max} data amount to transfer
     */
    private def loadStats(): String = {
        var stats = Set.empty[String]

        val ulLoads = ProcessingModel.allLoadUp()
        if (ulLoads.sum > 0.0) {
            val sortedUl = ulLoads.sorted
            val ulMedian = sortedUl.size match {
                case n if n % 2 == 0 =>
                    val upper = n / 2
                    ((sortedUl(upper - 1) + sortedUl(upper)) / 2.0)

                case n =>
                    sortedUl(n / 2)
            }

            stats += {
                "UL[MIN %.2f MAX %.2f MEDIAN %.2f]".
                    format(sortedUl.head, sortedUl.last, ulMedian)
            }
        }

        val dlLoads = ProcessingModel.allLoadDown()
        if (dlLoads.sum > 0.0) {
            val sortedDl = dlLoads.sorted
            val dlMedian = sortedDl.size match {
                case n if n % 2 == 0 =>
                    val upper = n / 2
                    ((sortedDl(upper - 1) + sortedDl(upper)) / 2)

                case n =>
                    sortedDl(n / 2)
            }

            stats += {
                "DL[MIN %.2f MAX %.2f MEDIAN %.2f]".
                    format(sortedDl.head, sortedDl.last, dlMedian)
            }
        }

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
            Log.line("SC", "Starting repair of %.3fMB".format(totalSize))
        } else {
            totalSizeOfRepair += totalSize
            Log.line("SC", "Adding repair of %.3fMB".format(totalSize))
        }

    def finishRepair() = {
        val totalTime = CloudSim.clock() - startOfRepair
        val totalMeanBW = totalSizeOfRepair / totalTime
        Log.line("SC", "Finished repair of %.3fMB in %.3f @ %.3fMbit/s".format(totalSizeOfRepair, totalTime, totalMeanBW * 8))
        repairInfos = Queue.empty
        startOfRepair = Double.NaN
        totalSizeOfRepair = Double.NaN
        finishedAmount = 0.0
    }

    def progressRepair(size: Double) = {
        repairInfos = repairInfos :+ RepairInfo(CloudSim.clock(), size)
        finishedAmount += size
    }

    private def repairStats(): String =
        if (!startOfRepair.isNaN) {
            val clock = CloudSim.clock()
            val leastIncludedClock = clock - 10.0
            repairInfos = repairInfos dropWhile { _.clock < leastIncludedClock }

            val bw10 = if (repairInfos.isEmpty) {
                0.0
            } else {
                { repairInfos map { _.size } sum } / { clock - repairInfos.head.clock }
            }

            val bwTotal = finishedAmount / (clock - startOfRepair)
            "REP[BW10 %.3f Mbit/s BWTOT %.3f Mbit/s]".format(bw10 * 8, bwTotal * 8)
        } else {
            ""
        }
}