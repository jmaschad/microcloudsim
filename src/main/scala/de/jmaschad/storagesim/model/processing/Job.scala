package de.jmaschad.storagesim.model.processing

import de.jmaschad.storagesim.Units

object Job {
    def apply(workloads: Set[Workload], onFinish: () => Unit) = new Job(workloads, onFinish);
}

class Job(workloads: Set[Workload], val onFinish: () => Unit) {
    def process(timeSpan: Double): Job = Job(workloads.map(_.process(timeSpan)), onFinish)
    def isDone: Boolean = workloads.forall(_.isDone)
    def expectedCompletion: Double = workloads.map(_.expectedCompletion).max
}