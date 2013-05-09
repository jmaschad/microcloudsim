package de.jmaschad.storagesim

import org.apache.commons.math3.distribution.IntegerDistribution
import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.apache.commons.math3.distribution.ExponentialDistribution
import org.apache.commons.math3.distribution.PoissonDistribution
import de.jmaschad.storagesim.model.user.UserBehavior
import de.jmaschad.storagesim.model.user.RequestType._
import java.io.PrintWriter
import org.apache.commons.math3.distribution.ZipfDistribution
import org.apache.commons.math3.distribution.WeibullDistribution
import org.apache.commons.math3.distribution.UniformRealDistribution

object RealDistributionConfiguration {
    def toDist(config: RealDistributionConfiguration) = config match {
        case NormalDist(mean, dev) => new NormalDistribution(mean, dev)
        case ExponentialDist(mean) => new ExponentialDistribution(mean)
        case WeibullDist(alpha, beta) => new WeibullDistribution(alpha, beta)
        case UniformRealDist(min, max) => new UniformRealDistribution(min, max)
        case _ => throw new IllegalStateException
    }
}
sealed abstract class RealDistributionConfiguration
case class NormalDist(mean: Double, deviation: Double) extends RealDistributionConfiguration
case class ExponentialDist(mean: Double) extends RealDistributionConfiguration
case class WeibullDist(alpha: Double, beta: Double) extends RealDistributionConfiguration
case class UniformRealDist(min: Double, max: Double) extends RealDistributionConfiguration

object IntegerDistributionConfiguration {
    def toDist(config: IntegerDistributionConfiguration): IntegerDistribution = config match {
        case PoissonDist(mean) => new PoissonDistribution(mean)
        case UniformIntDist(min, max) => new UniformIntegerDistribution(min, max)
        case ZipfDist(elems, exp) => new ZipfDistribution(elems, exp)
        case _ => throw new IllegalStateException
    }
}
sealed abstract class IntegerDistributionConfiguration
case class PoissonDist(mean: Double) extends IntegerDistributionConfiguration
case class UniformIntDist(min: Int, max: Int) extends IntegerDistributionConfiguration
case class ZipfDist(elems: Int, exp: Double) extends IntegerDistributionConfiguration

object BehaviorConfig {
    def apply(requestType: RequestType,
        delayModel: RealDistributionConfiguration) =
        new BehaviorConfig(requestType, delayModel)
}
class BehaviorConfig(
    val requestType: RequestType,
    val delayModel: RealDistributionConfiguration) {
    override def toString = requestType + " [" + delayModel + "]"
}

sealed abstract class SelectorConfig
case class RandomBucketBased extends SelectorConfig
case class RandomObjectBased extends SelectorConfig
case class PlacementBased extends SelectorConfig

object StorageSimConfig {
    def printDescription(configuration: StorageSimConfig, writer: PrintWriter): Unit = {
        writer.println("Configuration description:")
        //        writer.println("duration = " + configuration.simDuration)
        writer.println()
        writer.println("selector = " + configuration.selector.getClass().getSimpleName())
        writer.println("replica count = " + configuration.replicaCount)
        writer.println()
        writer.println("cloud count = " + configuration.cloudCount)
        writer.println("cloud bandwidth dist = " + configuration.cloudBandwidth)
        writer.println()
        writer.println("user count = " + configuration.userCount)
        writer.println("bucket count = " + configuration.bucketCount)
        writer.println("object size dist = " + configuration.objectSize)
        writer.println("mean get inteval dist = " + configuration.meanGetInterval)
        writer.println("object popularity model = " + configuration.objectPopularityModel)
    }
}

trait StorageSimConfig {
    var outputDir: String = "experiments"
    //    var simDuration: Double = 6.307e7 // two years

    var selector: SelectorConfig = PlacementBased()
    var replicaCount: Int = 3

    var regionCount: Int = 30
    var cloudCount: Int = 30
    var userCount: Int = 500

    var cloudBandwidth: RealDistributionConfiguration = NormalDist(125 * Units.MByte, 20 * Units.MByte)

    // number of buckets in the system
    var bucketCount: Int = 100

    // how many buckets can a user access
    var bucketsPerUser: IntegerDistributionConfiguration = PoissonDist(2)

    // size distribution of individual objects
    var objectSize: RealDistributionConfiguration = ExponentialDist(30)
    // which object will be selected for download
    var objectPopularityModel: RealDistributionConfiguration = ExponentialDist(1)

    // mttf of 2 hours
    //    var meanTimeToFailure: RealDistributionConfiguration = WeibullDist(0.7, 5688) // NormalDist(3.154e7, 1.0)

    // mean time interval between get requests
    var meanGetInterval: RealDistributionConfiguration = NormalDist(2, 0.5)
}