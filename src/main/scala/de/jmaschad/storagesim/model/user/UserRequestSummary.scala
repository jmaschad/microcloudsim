package de.jmaschad.storagesim.model.user

import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.processing.StorageObject
import org.cloudbus.cloudsim.core.SimEvent

object UserRequestSummary extends Enumeration {
    type RequestState = Value

    val Complete = Value("completed")
    val TimeOut = Value("timed out")
    val ObjectNotFound = Value("object not found")
    val ObjectExists = Value("object exists")
    val UnsufficientSpace = Value("unsufficient space")
    val NoOnlineClouds = Value("no online clouds")
    val CloudStorageError = Value("cloud storage error")
}
import UserRequestSummary._
