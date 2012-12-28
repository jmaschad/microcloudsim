package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.storage.StorageSystem

class ResourceProvisioning(storageSystem: StorageSystem, networkBandwidth: Double) {
  private val provisioner = List(new StorageIOProvisioner, new NetworkProvisioner)
  private var jobs: List[Job] = Nil
  private var lastUpdate: Option[Double] = None

  def add(job: Job): Unit = {
    jobs = job :: jobs
  }

  def update: Unit = {
    val clock = CloudSim.clock
    val timeElapsed = clock - lastUpdate.getOrElse(clock)
    lastUpdate = Some(clock)

    provisioner.foreach(_.update(timeElapsed, jobs))

    val done = jobs.filter(_.isDone)
    done.foreach(_.finish)
    jobs = jobs.diff(done)
  }

  def clear: Unit = {
    jobs.foreach(_.setFailed)
    update
  }

  trait Provisioner {
    def update(timeElapsed: Double, jobs: Seq[Job]): Unit = {}
  }

  class StorageIOProvisioner extends Provisioner {
    override def update(timeElapsed: Double, jobs: Seq[Job]): Unit =
      jobs.foreach(_ match {
        case job: UploadJob =>
          job.progressStorage(storageSystem.loadThroughput(job.storageObject) * timeElapsed)

        case job: DownloadJob =>
          job.progressStorage(storageSystem.storeThroughput(job.storageObject) * timeElapsed)
      })
  }

  class NetworkProvisioner extends Provisioner {
    override def update(timeElapsed: Double, jobs: Seq[Job]): Unit = {
      val bwPerJob = networkBandwidth / jobs.size
      jobs.foreach(_.progressNetwork(bwPerJob * timeElapsed))
    }
  }
}