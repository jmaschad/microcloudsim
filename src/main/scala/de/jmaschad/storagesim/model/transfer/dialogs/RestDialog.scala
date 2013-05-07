package de.jmaschad.storagesim.model.transfer.dialogs

import de.jmaschad.storagesim.model.StorageObject

object RequestSummary extends Enumeration {
    type RequestSummary = Value

    val Complete = Value("completed")
    val TimeOut = Value("timed out")
    val ObjectNotFound = Value("object not found")
    val ObjectExists = Value("object exists")
    val UnsufficientSpace = Value("unsufficient space")
    val CloudStorageError = Value("cloud storage error")
}
import RequestSummary._

object RestDialog {
    var requestId = 0L
    def nextId: Long = {
        val id = requestId
        requestId += 1
        id
    }
}

abstract sealed class RestDialog(val id: Long = RestDialog.nextId)
case class Get(obj: StorageObject) extends RestDialog
case class Delete(obj: StorageObject) extends RestDialog
case class RestAck extends RestDialog