package de.jmaschad.storagesim.model.microcloud

import org.apache.commons.math3.linear.ArrayRealVector
import org.cloudbus.cloudsim.core.CloudSim

import de.jmaschad.storagesim.model.Job
import de.jmaschad.storagesim.model.storage.StorageSystem

class ResourceProvisioning(storageSystem: StorageSystem, networkBandwidth: Double, cloud: MicroCloud) {
  private val provisioner = List(new NetUpProvisioner, new NetDownProvisioner, new IoLoadProvisioner, new IoStoreProvisioner)
  private var lastUpdate: Option[Double] = None

  var jobs = IndexedSeq.empty[Job]

  def add(job: Job): Unit = {
    update(false)
    jobs = job +: jobs
    scheduleNextUpdate()
  }

  def update(scheduleUpdate: Boolean = true) = {
    val clock = CloudSim.clock
    val timeElapsed = clock - lastUpdate.getOrElse(clock)
    lastUpdate = Some(clock)

    provisioner.foreach(_.update(timeElapsed, jobs))

    val done = for (job <- jobs if job.isDone) yield {
      job.finish()
      job
    }
    jobs = jobs.diff(done)

    if (scheduleUpdate) { scheduleNextUpdate() }
  }

  def clear() = {
    jobs.foreach(_.setFailed)
    update()
  }

  private def scheduleNextUpdate() = {
    if (jobs.nonEmpty) {
      assume(jobs.filter(_.isDone).isEmpty)

      val expectations = provisioner.map(_.expectedCompletions(jobs))
      val min = expectations.foldRight(new Array[Double](jobs.size))((a, b) => a.zip(b).map(pair => pair._1.max(pair._2))).min

      // min 100 microseconds delay to avoid infinite update loop with zero progress
      val minDelay = 0.0001
      cloud.scheduleProcessingUpdate(min.max(minDelay))
    }
  }

  private def bandwidthPerJob = networkBandwidth / jobs.size

  trait Provisioner {
    def update(timeElapsed: Double, jobs: Seq[Job])
    def expectedCompletions(jobs: Seq[Job]): Array[Double]
  }

  class NetUpProvisioner extends Provisioner {
    def update(timeElapsed: Double, jobs: Seq[Job]) = jobs.foreach(_.progressNetUp(timeElapsed * bandwidthPerJob))
    def expectedCompletions(jobs: Seq[Job]): Array[Double] = jobs.map(j => (j.netUpSize.getOrElse(0.0) / bandwidthPerJob).max(0)).toArray
  }

  class NetDownProvisioner extends Provisioner {
    def update(timeElapsed: Double, jobs: Seq[Job]) = jobs.foreach(_.progressNetDown(timeElapsed * bandwidthPerJob))
    def expectedCompletions(jobs: Seq[Job]): Array[Double] = jobs.map(j => (j.netDownSize.getOrElse(0.0) / bandwidthPerJob).max(0)).toArray
  }

  class IoLoadProvisioner extends Provisioner {
    def update(timeElapsed: Double, jobs: Seq[Job]) = jobs.foreach(j => j.progressIoLoad(timeElapsed * storageSystem.loadThroughput(j.storageObject)))
    def expectedCompletions(jobs: Seq[Job]): Array[Double] =
      jobs.map(j => (j.ioLoadSize.getOrElse(0.0) / storageSystem.loadThroughput(j.storageObject)).max(0)).toArray
  }

  class IoStoreProvisioner extends Provisioner {
    def update(timeElapsed: Double, jobs: Seq[Job]) = jobs.foreach(j => j.progressIoStore(timeElapsed * storageSystem.storeThroughput(j.storageObject)))
    def expectedCompletions(jobs: Seq[Job]): Array[Double] =
      jobs.map(j => (j.ioStoreSize.getOrElse(0.0) / storageSystem.storeThroughput(j.storageObject)).max(0)).toArray
  }
}