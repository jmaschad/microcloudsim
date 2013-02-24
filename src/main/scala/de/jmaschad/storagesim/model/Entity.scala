package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity

object Entity {
    def entityForId(id: Int): Entity = CloudSim.getEntity(id) match {
        case e: Entity => e
        case _ => throw new IllegalStateException
    }
}

abstract class Entity(name: String, val region: Int) extends SimEntity(name) {
    /**
     * This will be called when an entity gets killed.
     * It should reset the entities state to its initial state.
     */
    protected def reset(): Unit

    /**
     * Create entity specific log messages
     */
    protected def log(line: String): Unit
}