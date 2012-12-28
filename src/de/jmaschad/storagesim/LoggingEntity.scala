package de.jmaschad.storagesim

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity

trait LoggingEntity extends SimEntity {
  def log(message: String): Unit = {
    println("%.3f %s '%s': %s".format(CloudSim.clock(), getClass.getSimpleName(), getName(), message))
  }
}