package de.jmaschad.storagesim.model.processing

import scala.Array.canBuildFrom
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.Units

object ProcessingModel {
    private val Base = 10500
    val ProcUpdate = Base + 1
}

class ProcessingModel(
    log: String => Unit,
    scheduleUpdate: Double => Unit,
    totalBandwidth: Double) {

    abstract class Transfer(size: Double, val onFinish: () => Unit) {
        def progress(timespan: Double): Transfer
        def expectedDuration: Double
        def isDone: Boolean = size < 1 * Units.Byte
    }

    class Download(size: Double, onFinish: () => Unit) extends Transfer(size, onFinish) {
        def progress(timespan: Double): Transfer = new Download(size - (timespan * downloadBandwidth), onFinish)
        def expectedDuration: Double = size / downloadBandwidth
    }

    class Upload(size: Double, onFinish: () => Unit) extends Transfer(size, onFinish) {
        def progress(timespan: Double): Transfer = new Upload(size - (timespan * uploadBandwidth), onFinish)
        def expectedDuration: Double = size / uploadBandwidth
    }

    private var lastUpdate = 0.0
    private var transfers = Set.empty[Transfer]
    private var uploadCount = 0
    private var downloadCount = 0

    def jobCount = transfers.size

    private def uploadBandwidth: Double = totalBandwidth / uploadCount
    private def downloadBandwidth: Double = totalBandwidth / downloadCount

    def download(size: Double, onFinish: () => Unit) = {
        downloadCount += 1
        add(new Download(size, onFinish))
    }

    def upload(size: Double, onFinish: () => Unit) = {
        uploadCount += 1
        add(new Upload(size, onFinish))
    }

    def update(scheduleUpdate: Boolean = true) = {
        val timeElapsed = timeSinceLastUpdate
        transfers = transfers map { _.progress(timeElapsed) }
        lastUpdate = CloudSim.clock()

        val done = transfers filter { _.isDone }
        done foreach { transfer =>
            transfer.onFinish()
            transfer match {
                case _: Download => downloadCount -= 1
                case _: Upload => uploadCount -= 1
            }
        }
        transfers = transfers diff done

        if (scheduleUpdate) { scheduleNextUpdate() }
    }

    def reset(): Unit = {
        lastUpdate = 0.0
        transfers = Set.empty
        uploadCount = 0
        downloadCount = 0
    }

    private def add(transfer: Transfer): Unit = {
        val timeElapsed = timeSinceLastUpdate
        if (timeElapsed > 0.0) {
            update(false)
        }
        transfers += transfer
        scheduleNextUpdate()
    }

    // provide a minimum delay to avoid infinite update loop with zero progress
    private def scheduleNextUpdate() =
        if (transfers.nonEmpty) {
            val expected = { transfers map { _.expectedDuration } min } max (0.0001)
            scheduleUpdate(expected)
        }

    private def timeSinceLastUpdate: Double = {
        val clock = CloudSim.clock
        val time = clock - lastUpdate
        assert(time >= 0)
        time
    }
}