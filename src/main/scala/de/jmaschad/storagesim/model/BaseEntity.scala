package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log

class BaseEntity(name: String) extends Entity(name) {
    override def startEntity(): Unit = {}
    override def shutdownEntity(): Unit = {}
    override def processEvent(event: SimEvent): Unit = {
        log("dropped event: " + event)
    }

    protected override def reset(): Unit =
        log("was reset")

    protected override def log(message: String): Unit =
        Log.line(getClass.getSimpleName + " '" + getName + "'", message)
}