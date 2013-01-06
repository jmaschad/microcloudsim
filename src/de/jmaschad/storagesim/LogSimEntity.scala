package de.jmaschad.storagesim

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity

abstract class LogSimEntity(name: String) extends SimEntity(name) {
  override def startEntity() = log("started")
  override def shutdownEntity() = log("shutdown")

  protected def log(message: String): Unit = {
    println("%.3f %s '%s': %s".format(CloudSim.clock(), getClass.getSimpleName(), getName(), message))
  }
}