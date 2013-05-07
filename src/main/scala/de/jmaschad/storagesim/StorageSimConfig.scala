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

object RealDistributionConfiguration {
    def toDist(config: RealDistributionConfiguration) = config match {
        case NormalDist(mean, dev) => new NormalDistribution(mean, dev)
        case ExponentialDist(mean) => new ExponentialDistribution(mean)
        case WeibullDist(alpha, beta) => new WeibullDistribution(alpha, beta)
        case _ => throw new IllegalStateException
    }
}
sealed abstract class RealDistributionConfiguration
case class NormalDist(mean: Double, deviation: Double) extends RealDistributionConfiguration
case class ExponentialDist(mean: Double) extends RealDistributionConfiguration
case class WeibullDist(alpha: Double, beta: Double) extends RealDistributionConfiguration

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

object ObjectSelectionModel {
    def toDist(objectCount: Int, config: ObjectSelectionModel): IntegerDistribution = config match {
        case UniformSelection() => new UniformIntegerDistribution(0, objectCount - 1)
        case ZipfSelection() => new ZipfDistribution(objectCount - 1, 1)
        case _ => throw new IllegalStateException
    }
}
sealed abstract class ObjectSelectionModel
case class UniformSelection extends ObjectSelectionModel
case class ZipfSelection extends ObjectSelectionModel

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
case class GreedyBucketBased extends SelectorConfig
case class GreedyFileBased extends SelectorConfig

object StorageSimConfig {
    def printDescription(configuration: StorageSimConfig, writer: PrintWriter): Unit = {
        writer.println("Configuration description:")
        writer.println("duration = " + configuration.simDuration)
        writer.println()
        writer.println("selector = " + configuration.selector.getClass().getSimpleName())
        writer.println("replica count = " + configuration.replicaCount)
        writer.println()
        writer.println("cloud count = " + configuration.cloudCount)
        writer.println("cloud bandwidth dist = " + configuration.cloudBandwidth)
        writer.println()
        writer.println("user count = " + configuration.userCount)
        writer.println("bucket count = " + configuration.bucketCount)
        writer.println("objects per bucket dist = " + configuration.bucketSizeType)
        writer.println("object size dist = " + configuration.objectSize)
        writer.println("mean get inteval dist = " + configuration.meanGetInterval)
        writer.println("object for get selection = " + configuration.getTargetModel)
    }
}

trait StorageSimConfig {
    var passCount: Int = 1
    var outputDir: String = "/Users/wanbird/Documents/Grosser Beleg/Experimente/"
    var simDuration: Double = 3.154e7 // one year

    var selector: SelectorConfig = RandomObjectBased()
    var replicaCount: Int = 3

    var regionCount: Int = 50
    var cloudCount: Int = 50
    var userCount: Int = 2000

    var cloudBandwidth: RealDistributionConfiguration = NormalDist(125 * Units.MByte, 20 * Units.MByte)

    // number of buckets in the system
    var bucketCount: Int = 200
    // how many buckets can a user access
    var bucketsPerUser: IntegerDistributionConfiguration = PoissonDist(2)
    // 3 bucket sizes: small, medium, large
    var bucketSizeType: IntegerDistributionConfiguration = ZipfDist(3, 2)
    // size distribution of individual objects
    var objectSize: RealDistributionConfiguration = WeibullDist(1.2, 10)

    var meanTimeToFailure: RealDistributionConfiguration = NormalDist(86400, 3600)
    var meanTimeToReplace: RealDistributionConfiguration = NormalDist(300, 15)

    // mean time interval between get requests
    var meanGetInterval: RealDistributionConfiguration = NormalDist(2, 0.5)
    // which object will be selected for download
    var getTargetModel: ObjectSelectionModel = ZipfSelection()
}