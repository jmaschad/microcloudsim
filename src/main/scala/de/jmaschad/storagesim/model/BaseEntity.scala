package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log

class BaseEntity(name: String) extends SimEntity(name) {
    override def startEntity(): Unit = {}
    override def shutdownEntity(): Unit = {}
    override def processEvent(event: SimEvent): Unit = {
        log("dropped event: " + event)
    }

    protected def log(message: String): Unit = Log.line(getClass.getSimpleName + " '" + getName + "'", message)
}