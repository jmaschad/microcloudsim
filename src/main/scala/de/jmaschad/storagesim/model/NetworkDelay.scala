package de.jmaschad.storagesim.model

import scala.math._
import org.apache.commons.math3.distribution.NormalDistribution
import de.jmaschad.storagesim.StorageSimConfig
import de.jmaschad.storagesim.StorageSim
import org.cloudbus.cloudsim.NetworkTopology

object NetworkDelay {
    val regionDelay = new NormalDistribution(0.1, 0.03)
    /**
     * Computes a networking delay between two regions.
     * The delay is a function of the numerical difference
     * between regionA and regionB.
     *
     * If one of the regions is 0 the delay will be 0 as well.
     */
    def between(regionA: Int, regionB: Int): Double =
        if (regionA == 0 || regionB == 0) {
            0.0f
        } else if (regionA == regionB) {
            regionDelay.sample().max(0.001)
        } else {
            NetworkTopology.getDelay(regionA, regionB)
        }

}