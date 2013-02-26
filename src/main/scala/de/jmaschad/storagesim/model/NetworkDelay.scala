package de.jmaschad.storagesim.model

import scala.math._
import org.apache.commons.math3.distribution.NormalDistribution
import de.jmaschad.storagesim.StorageSimConfig
import de.jmaschad.storagesim.StorageSim

object NetworkDelay {
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
        } else {
            var dist = 0
            // poor man's finite field difference 
            while (((regionA + dist) % (StorageSim.configuration.regionCount + 1)) != regionB)
                dist += 1

            val median = (exp(dist + 1) * 0.014) + 0.02
            val delay = new NormalDistribution(median, 0.05 * median).sample().max(0.005)
            delay
        }
}