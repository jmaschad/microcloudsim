package de.jmaschad.storagesim

import org.apache.commons.math3.distribution.UniformIntegerDistribution

object RandomUtils {
    def randomSelect1[T](values: IndexedSeq[T]): T = distinctRandomSelectN(1, values).head

    def distinctRandomSelectN[T](count: Int, values: IndexedSeq[_ <: T]): Set[T] = {
        assert(count > 0)
        assert(count <= values.size)

        values.size match {
            case 1 => Set(values.head)

            case n =>
                val dist = new UniformIntegerDistribution(0, n - 1)
                var distinctSample = Set.empty[Int]
                while (distinctSample.size < count) {
                    distinctSample += dist.sample()
                }
                distinctSample.map(values(_))
        }
    }
}