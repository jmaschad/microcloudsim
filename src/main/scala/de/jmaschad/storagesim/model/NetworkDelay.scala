package de.jmaschad.storagesim.model

import scala.math._
import org.apache.commons.math3.distribution.NormalDistribution

object NetworkDelay {
    /**
     * Computes a networking delay between two regions.
     * The delay is a function of the numerical difference
     * between regionA and regionB.
     *
     * If one of the regions is 0 the delay will be 0 as well.
     */
    def delayBetween(regionA: Int, regionB: Int): Double =
        if (regionA == 0 || regionB == 0) {
            0.0f
        } else {
            val diff = (regionA - regionB).abs + 1
            val median = 0.1 * diff
            val stdDev = 0.01 * diff
            val delay = new NormalDistribution(median, stdDev).sample().max(0.02)
            //            println("added network delay: %.3f [dist %d stdDev %.3f]".format(delay, diff, stdDev))
            delay
        }
}