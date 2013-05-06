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

object RealDistributionConfiguration {
    def toDist(config: RealDistributionConfiguration) = config match {
        case NormalDist(mean, dev) => new NormalDistribution(mean, dev)
        case ExponentialDist(mean) => new ExponentialDistribution(mean)
        case _ => throw new IllegalStateException
    }
}
sealed abstract class RealDistributionConfiguration
case class NormalDist(mean: Double, deviation: Double) extends RealDistributionConfiguration
case class ExponentialDist(mean: Double) extends RealDistributionConfiguration

object IntegerDistributionConfiguration {
    def toDist(config: IntegerDistributionConfiguration): IntegerDistribution = config match {
        case PoissonDist(mean) => new PoissonDistribution(mean)
        case UniformIntDist(min, max) => new UniformIntegerDistribution(min, max)
        case _ => throw new IllegalStateException
    }
}
sealed abstract class IntegerDistributionConfiguration
case class PoissonDist(mean: Double) extends IntegerDistributionConfiguration
case class UniformIntDist(min: Int, max: Int) extends IntegerDistributionConfiguration

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
        writer.println("cloud bandwidth dist = " + configuration.cloudBandwidthDistribution)
        writer.println()
        writer.println("user count = " + configuration.userCount)
        writer.println("bucket count = " + configuration.bucketCount)
        writer.println("object count dist = " + configuration.objectCountDistribution)
        writer.println("object size dist = " + configuration.objectSizeDistribution)
        writer.println("median get delay dist = " + configuration.medianGetDelayDistribution)
        writer.println("object for get selection = " + configuration.objectForGetDistribution)
    }
}

trait StorageSimConfig {
    var passCount: Int = 1
    var outputDir: String = "/Users/wanbird/Documents/Grosser Beleg/Experimente"
    var simDuration: Double = 60.0 * 60.0 * 24.0

    var selector: SelectorConfig = RandomObjectBased()
    var replicaCount: Int = 3

    var regionCount: Int = 40
    var cloudCount: Int = 100
    var storageDevicesPerCloud: Int = 10
    var cloudBandwidthDistribution: RealDistributionConfiguration = NormalDist(125 * Units.MByte, 20 * Units.MByte)

    var meanTimeToFailureDistribution: RealDistributionConfiguration = NormalDist(100 * 60, 1)
    var meanTimeToReplaceDistribution: RealDistributionConfiguration = NormalDist(30, 5)

    var userCount: Int = 1000
    var bucketCount: Int = (userCount * 0.3).toInt
    var accessedBucketCountDist: IntegerDistributionConfiguration = PoissonDist(1)
    var objectCountDistribution: IntegerDistributionConfiguration = PoissonDist(100)
    var objectSizeDistribution: RealDistributionConfiguration = ExponentialDist(5 * Units.MByte)

    var medianGetDelayDistribution: RealDistributionConfiguration = NormalDist(2, 0.5)
    var objectForGetDistribution: ObjectSelectionModel = ZipfSelection()
}