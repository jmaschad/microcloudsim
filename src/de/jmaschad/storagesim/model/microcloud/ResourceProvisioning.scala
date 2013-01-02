package de.jmaschad.storagesim.model.microcloud

import org.apache.commons.math3.linear.ArrayRealVector
import org.cloudbus.cloudsim.core.CloudSim

import de.jmaschad.storagesim.model.DownloadJob
import de.jmaschad.storagesim.model.Job
import de.jmaschad.storagesim.model.UploadJob
import de.jmaschad.storagesim.model.storage.StorageSystem

class ResourceProvisioning(storageSystem: StorageSystem, networkBandwidth: Double, cloud: MicroCloud) {
  private val provisioner = List(new StorageIOProvisioner, new NetworkProvisioner)
  private var jobs: List[Job] = Nil
  private var lastUpdate: Option[Double] = None

  def add(job: Job): Unit = {
    update()
    jobs = job :: jobs
    scheduleNextUpdate()
  }

  def update() = {
    val clock = CloudSim.clock
    val timeElapsed = clock - lastUpdate.getOrElse(clock)
    lastUpdate = Some(clock)

    provisioner.foreach(_.update(timeElapsed, jobs))

    val done = jobs.filter(_.isDone)
    done.foreach(_.finish)
    jobs = jobs.diff(done)

    if (done.nonEmpty) {
      cloud.log("finished job: " + done.head)
      scheduleNextUpdate()
    }
  }

  def clear() = {
    jobs.foreach(_.setFailed)
    update
  }

  private def scheduleNextUpdate() = {
    var expectations = new ArrayRealVector(jobs.size);
    for (prov <- provisioner) {
      expectations = expectations.add(new ArrayRealVector(prov.expectedCompletions(jobs)));
    }

    // add 100 microseconds to avoid "early" updates because of rounding
    // errors
    cloud.scheduleProcessingUpdate(expectations.getMinValue() + 0.0001);
  }

  trait Provisioner {
    def update(timeElapsed: Double, jobs: Seq[Job])
    def expectedCompletions(jobs: Seq[Job]): Array[Double]
  }

  class StorageIOProvisioner extends Provisioner {
    override def update(timeElapsed: Double, jobs: Seq[Job]): Unit =
      jobs.foreach(job => job match {
        case UploadJob(obj, _) =>
          job.progressStorage(storageSystem.loadThroughput(obj) * timeElapsed)

        case DownloadJob(obj, _) =>
          job.progressStorage(storageSystem.storeThroughput(obj) * timeElapsed)

        case _ =>
          throw new IllegalStateException
      })

    override def expectedCompletions(jobs: Seq[Job]): Array[Double] =
      jobs.map(job => job match {
        case UploadJob(obj, _) =>
          obj.size / storageSystem.loadThroughput(obj)

        case DownloadJob(obj, _) =>
          obj.size / storageSystem.storeThroughput(obj)

        case _ =>
          throw new IllegalStateException
      }).map(i => if (i < 0) 0 else i).toArray
  }

  class NetworkProvisioner extends Provisioner {
    override def update(timeElapsed: Double, jobs: Seq[Job]) = {
      val bwPerJob = networkBandwidth / jobs.size
      jobs.foreach(_.progressNetwork(bwPerJob * timeElapsed))
    }

    override def expectedCompletions(jobs: Seq[Job]): Array[Double] =
      jobs.map(job => job.transferSize / (networkBandwidth / jobs.size)).map(i => if (i < 0) 0 else i).toArray

  }
}