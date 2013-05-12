package de.jmaschad.storagesim.model

import scala.math._
import org.apache.commons.math3.distribution.NormalDistribution
import de.jmaschad.storagesim.StorageSimConfig
import de.jmaschad.storagesim.StorageSim
import org.cloudbus.cloudsim.NetworkTopology
import org.apache.commons.math3.distribution.WeibullDistribution

object NetworkDelay {
    val regionDelay = new WeibullDistribution(2, 40)
    /**
     * Computes a networking delay between two regions.
     * The delay is a function of the numerical difference
     * between regionA and regionB.
     *
     * If one of the regions is 0 the delay will be 0 as well.
     */
    def between(source: Int, target: Int): Double =
        if (source == 0 || target == 0) {
            0.0f
        } else {
            NetworkTopology.getDelay(source, target)
        }

}