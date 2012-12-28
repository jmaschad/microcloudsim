package de.jmaschad.storagesim.model

import de.jmaschad.storagesim.Units
import de.jmaschad.storagesim.model.storage.StorageObject

protected class Job(transferSize: Double, onFinish: Boolean => Unit) {
  private var success = true;
  private var storageTransfer = transferSize;
  private var networkTransfer = transferSize;

  def progressNetwork(progress: Double) = { networkTransfer -= progress }
  def progressStorage(progress: Double) = { storageTransfer -= progress }

  def setFailed = { success = false }
  def hasFailed = !success
  def finish: Unit = onFinish(success)
  def isDone = storageTransfer < 1 * Units.Byte && networkTransfer < 1 * Units.Byte
}

case class UploadJob(storageObject: StorageObject, onFinish: Boolean => Unit) extends Job(storageObject.size, onFinish)
case class DownloadJob(storageObject: StorageObject, onFinish: Boolean => Unit) extends Job(storageObject.size, onFinish)