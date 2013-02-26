package de.jmaschad.storagesim

import org.apache.commons.math3.distribution.ZipfDistribution

object DistTest {
    def main(args: Array[String]): Unit = {
        val dist = new ZipfDistribution(100, 1)
        val hist = scala.collection.mutable.Map.empty[Int, Int]
        (1 to 10000) foreach (_ => {
            val sample = dist.sample()
            hist(sample) = hist.getOrElse(sample, 0) + 1
        })
        println(hist.mkString("\n"))
    }
}