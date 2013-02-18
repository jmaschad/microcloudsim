package de.jmaschad.storagesim.model.processing

import de.jmaschad.storagesim.Units

object Job {
    def apply(workloads: Set[_ <: Workload], onFinish: () => Unit) = new Job(workloads, onFinish);
}

class Job(workloads: Set[_ <: Workload], val onFinish: () => Unit) {
    def process(timeSpan: Double): Job = Job(workloads.map(_.process(timeSpan)), onFinish)
    def isDone: Boolean = workloads.forall(_.isDone)
    def expectedDuration: Double = workloads.map(_.expectedDuration).max
}