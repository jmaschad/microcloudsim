package de.jmaschad.storagesim.model.processing

import scala.Array.canBuildFrom
import org.cloudbus.cloudsim.core.CloudSim

object ProcessingModel {
    private val Base = 10500
    val ProcUpdate = Base + 1
}

class ProcessingModel(
    log: String => Unit,
    scheduleUpdate: Double => Unit,
    totalBandwidth: Double) {

    private var lastUpdate = 0.0
    private var jobs = Set.empty[Job]
    private var uploadCount = 0
    private var downloadCount = 0

    def jobCount = jobs.size

    private def uploadBandwidth: Double = totalBandwidth / uploadCount
    private def downloadBandwidth: Double = totalBandwidth / downloadCount

    def download(size: Double, onFinish: () => Unit) = {
        val workloads = Set(NetDown(size, { downloadBandwidth }))
        downloadCount += 1
        add(Job(workloads, onFinish))
    }

    def upload(size: Double, onFinish: () => Unit) = {
        val workloads = Set(NetUp(size, { uploadBandwidth }))
        uploadCount += 1
        add(Job(workloads, onFinish))
    }

    def update(scheduleUpdate: Boolean = true) = {
        val timeElapsed = timeSinceLastUpdate
        jobs = jobs map { _.process(timeElapsed) }
        lastUpdate = CloudSim.clock()

        val done = jobs filter { _.isDone }
        done foreach { job =>
            job.onFinish()
            if ({ job.workloads collect { case dl: NetDown => dl } size } > 0) downloadCount -= 1
            if ({ job.workloads collect { case ul: NetUp => ul } size } > 0) uploadCount -= 1
        }
        jobs = jobs diff done

        if (scheduleUpdate) { scheduleNextUpdate() }
    }

    def reset(): Unit = {
        lastUpdate = 0.0
        jobs = Set.empty
        uploadCount = 0
        downloadCount = 0
    }

    private def add(job: Job): Unit = {
        val timeElapsed = timeSinceLastUpdate
        if (timeElapsed > 0.0) {
            update(false)
        }
        jobs += job
        scheduleNextUpdate()
    }

    // provide a minimum delay to avoid infinite update loop with zero progress
    private def scheduleNextUpdate() =
        if (jobs.nonEmpty) {
            val expected = { jobs map { _.expectedDuration } min } max (0.0001)
            scheduleUpdate(expected)
        }

    private def timeSinceLastUpdate: Double = {
        val clock = CloudSim.clock
        val time = clock - lastUpdate
        assert(time >= 0)
        time
    }
}