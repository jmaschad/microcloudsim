package de.jmaschad.storagesim.model

import scala.math._
import org.apache.commons.math3.distribution.NormalDistribution
import de.jmaschad.storagesim.StorageSimConfig
import de.jmaschad.storagesim.StorageSim
import org.cloudbus.cloudsim.NetworkTopology
import org.apache.commons.math3.distribution.WeibullDistribution

object NetworkDelay {
    def between(source: Int, target: Int): Double =
        if (source == 0 || target == 0) {
            0.0f
        } else {
            NetworkTopology.getDelay(source, target)
        }

}