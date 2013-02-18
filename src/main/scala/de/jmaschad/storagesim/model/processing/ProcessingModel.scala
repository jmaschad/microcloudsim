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
    private var nextUpdate = 0.0
    private var jobs = Set.empty[Job]

    def jobCount = jobs.size

    def add(job: Job): Unit = {
        val timeElapsed = timeSinceLastUpdate
        if (timeElapsed >= nextUpdate) {
            update(false)
        }
        jobs += job
        scheduleNextUpdate()
    }

    def download(size: Double, onFinish: () => Unit) = {
        val workloads = Set(Download(size, totalBandwidth))
        add(Job(workloads, onFinish))
    }

    def upload(size: Double, onFinish: () => Unit) = {
        val workloads = Set(Upload(size, totalBandwidth))
        add(Job(workloads, onFinish))
    }

    def downloadAndStore(size: Double, transaction: StorageTransaction, onFinish: () => Unit) = {
        val workloads = Set[Workload](
            Download(size, totalBandwidth),
            DiskIO(size, { transaction.throughput }))
        add(Job(workloads, onFinish))
    }

    def loadAndUpload(size: Double, transaction: StorageTransaction, onFinish: () => Unit) = {
        val workloads = Set[Workload](
            Upload(size, totalBandwidth),
            DiskIO(size, { transaction.throughput }))
        add(Job(workloads, onFinish))
    }

    def update(scheduleUpdate: Boolean = true) = {
        val timeElapsed = timeSinceLastUpdate
        jobs = jobs.map(_.process(timeElapsed))
        lastUpdate = CloudSim.clock()

        val done = jobs.filter(_.isDone)
        done.foreach(_.onFinish())
        jobs --= done

        if (scheduleUpdate) { scheduleNextUpdate() }
    }

    def reset(): ProcessingModel = new ProcessingModel(log, scheduleUpdate, totalBandwidth)

    // provide a minimum delay to avoid infinite update loop with zero progress
    private def scheduleNextUpdate() =
        if (jobs.nonEmpty) {
            nextUpdate = jobs.map(_.expectedDuration).min.max(0.0001)
            scheduleUpdate(nextUpdate)
        }

    private def timeSinceLastUpdate: Double = {
        val clock = CloudSim.clock
        val time = clock - lastUpdate
        assert(time >= 0)
        time
    }
}