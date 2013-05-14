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
import org.apache.commons.math3.distribution.BinomialDistribution

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
        case BinomialDist(trials, p) => new BinomialDistribution(trials, p)
        case _ => throw new IllegalStateException
    }
}
sealed abstract class IntegerDistributionConfiguration
case class PoissonDist(mean: Double) extends IntegerDistributionConfiguration
case class UniformIntDist(min: Int, max: Int) extends IntegerDistributionConfiguration
case class ZipfDist(elems: Int, exp: Double) extends IntegerDistributionConfiguration
case class BinomialDist(trials: Int, p: Double) extends IntegerDistributionConfiguration

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
case class DynamicPlacementBased extends SelectorConfig

object StorageSimConfig {
    def logDescription(configuration: StorageSimConfig): Unit = {
        Log.line("CONF", "Configuration description:")
        Log.line("CONF", "selector = " + configuration.selector.getClass().getSimpleName())
        Log.line("CONF", "cloud count = " + configuration.cloudCount)
        Log.line("CONF", "cloud bandwidth dist = " + configuration.cloudBandwidth)
        Log.line("CONF", "user count = " + configuration.userCount)
        Log.line("CONF", "user bandwidth dist = " + configuration.userBandwidth)
        Log.line("CONF", "mean number of objects per user = " + configuration.meanObjectCount)
        Log.line("CONF", "bucket count = " + configuration.bucketCount)
        Log.line("CONF", "object size dist = " + configuration.objectSize)
        Log.line("CONF", "object popularity model = " + configuration.objectPopularityModel)
    }
}

trait StorageSimConfig {
    var outputDir: String = "experiments"
    //    var simDuration: Double = 6.307e7 // two years

    var selector: SelectorConfig = DynamicPlacementBased()

    var replicaCount: Int = 3

    var cloudCount: Int = 20
    var userCount: Int = 260

    var cloudBandwidth: RealDistributionConfiguration = NormalDist(125 * Units.MByte, 0.25 * Units.MByte)
    var userBandwidth: RealDistributionConfiguration = NormalDist(4 * Units.MByte, 0.001 * Units.MByte)

    // number of buckets in the system
    var bucketCount: Int = 1000
    var bucketSizeDist: RealDistributionConfiguration = ExponentialDist(1)

    // the mean count of objects a user accesses 
    var meanObjectCount: Int = 20

    // size distribution of individual objects
    var objectSize: RealDistributionConfiguration = ExponentialDist(30)

    // what percentage of the user population will access a given object.
    var objectPopularityModel: RealDistributionConfiguration = ExponentialDist(0.05)

    // place objects on closely placed users
    var closePlacement: Boolean = false

    // mttf of 2 hours
    //    var meanTimeToFailure: RealDistributionConfiguration = WeibullDist(0.7, 5688) // NormalDist(3.154e7, 1.0)
}