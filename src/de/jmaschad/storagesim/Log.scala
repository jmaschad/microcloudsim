package de.jmaschad.storagesim

import org.cloudbus.cloudsim.core.CloudSim

object Log {
  def line(identifier: String, line: String) = println("%.3f %s: %s".format(CloudSim.clock(), identifier, line))
}