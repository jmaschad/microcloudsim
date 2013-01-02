package de.jmaschad.storagesim.model

import de.jmaschad.storagesim.Units
import de.jmaschad.storagesim.model.storage.StorageObject

abstract class Job(val transferSize: Double, onFinish: Boolean => Unit) {
  private var success = true;
  protected var storageTransfer = transferSize;
  protected var networkTransfer = transferSize;

  def progressNetwork(progress: Double) = { networkTransfer -= progress }
  def progressStorage(progress: Double) = { storageTransfer -= progress }

  def setFailed = { success = false }
  def hasFailed = !success
  def finish: Unit = onFinish(success)
  def isDone = storageTransfer < 1 * Units.Byte && networkTransfer < 1 * Units.Byte
}

case class UploadJob(storageObject: StorageObject, onFinish: Boolean => Unit) extends Job(storageObject.size, onFinish) {
  override def toString = "UploadJob %s [%f/%f]".format(storageObject, storageTransfer, networkTransfer)
}

case class DownloadJob(storageObject: StorageObject, onFinish: Boolean => Unit) extends Job(storageObject.size, onFinish) {
  override def toString = "DownloadJob %s".format(storageObject)
}