package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity

abstract class Entity(name: String) extends SimEntity(name) {
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