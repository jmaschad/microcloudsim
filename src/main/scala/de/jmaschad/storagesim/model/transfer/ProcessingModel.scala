package de.jmaschad.storagesim.model.transfer

import scala.Array.canBuildFrom

import org.cloudbus.cloudsim.core.CloudSim

class ProcessingModel(
    log: String => Unit,
    scheduleUpdate: Double => Unit) {
    private var lastUpdate: Option[Double] = None
    private var jobs = Set.empty[Job]

    def jobCount = jobs.size

    def add(job: Job): Unit = {
        update(false)
        jobs += job
        scheduleNextUpdate()
    }

    def update(scheduleUpdate: Boolean = true) = {
        val clock = CloudSim.clock
        val timeElapsed = clock - lastUpdate.getOrElse(clock)
        lastUpdate = Some(clock)

        jobs = jobs.map(_.process(timeElapsed))

        val done = jobs.filter(_.isDone)
        done.foreach(_.onFinish())
        jobs = jobs.diff(done)

        if (scheduleUpdate) { scheduleNextUpdate() }
    }

    def clear() = {
        jobs = Set.empty[Job]
    }

    // provide a minimum delay to avoid infinite update loop with zero progress
    private def scheduleNextUpdate() =
        if (jobs.nonEmpty) scheduleUpdate(jobs.map(_.expectedCompletion).min.max(0.0001))

}