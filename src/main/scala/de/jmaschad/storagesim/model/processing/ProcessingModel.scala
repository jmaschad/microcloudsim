package de.jmaschad.storagesim.model.processing

import scala.Array.canBuildFrom

import org.cloudbus.cloudsim.core.CloudSim

class ProcessingModel(
    log: String => Unit,
    scheduleUpdate: Double => Unit,
    totalBandwidth: Double) {
    private var lastUpdate: Option[Double] = None
    private var jobs = Set.empty[Job]

    def jobCount = jobs.size

    def add(job: Job): Unit = {
        if (timeSinceLastUpdae > 0.0) {
            update(false)
        }
        jobs += job
        scheduleNextUpdate()
    }

    def addObjectDownload(size: Double, transaction: StoreTransaction, onFinish: () => Unit) = {
        val workloads = Set[Workload](
            Download(size, totalBandwidth),
            DiskIO(size, { transaction.throughtput }))
        add(Job(workloads, () => {
            transaction.complete()
            onFinish()
        }))
    }

    def addObjectUpload(storageObject: StorageObject, transaction: LoadTransaction, onFinish: () => Unit) = {
        val workloads = Set[Workload](
            Upload(storageObject.size, totalBandwidth),
            DiskIO(storageObject.size, { transaction.throughput }))
        add(Job(workloads, () => {
            transaction.complete()
            onFinish()
        }))
    }

    def update(scheduleUpdate: Boolean = true) = {
        jobs = jobs.map(_.process(timeSinceLastUpdae))

        val done = jobs.filter(_.isDone)
        done.foreach(_.onFinish())
        jobs --= done

        if (scheduleUpdate) { scheduleNextUpdate() }

        lastUpdate = Some(CloudSim.clock())
    }

    def clear() = {
        jobs = Set.empty[Job]
    }

    // provide a minimum delay to avoid infinite update loop with zero progress
    private def scheduleNextUpdate() =
        if (jobs.nonEmpty) scheduleUpdate(jobs.map(_.expectedCompletion).min.max(0.0001))

    private def timeSinceLastUpdae: Double = {
        val clock = CloudSim.clock
        clock - lastUpdate.getOrElse(clock)
    }
}