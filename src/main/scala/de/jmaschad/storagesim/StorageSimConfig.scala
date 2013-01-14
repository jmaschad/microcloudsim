package de.jmaschad.storagesim

import org.apache.commons.math3.distribution.IntegerDistribution
import org.apache.commons.math3.distribution.RealDistribution
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.model.request.RequestType._
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.apache.commons.math3.distribution.ExponentialDistribution
import org.apache.commons.math3.distribution.PoissonDistribution
import de.jmaschad.storagesim.model.request.RequestType

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
        case _ => throw new IllegalStateException
    }
}
sealed abstract class IntegerDistributionConfiguration
case class PoissonDist(mean: Double) extends IntegerDistributionConfiguration

object ObjectSelectionModel {
    def toDist(objectCount: Int, config: ObjectSelectionModel): IntegerDistribution = config match {
        case UniformSelection() => new UniformIntegerDistribution(0, objectCount - 1)
        case _ => throw new IllegalStateException
    }
}
sealed abstract class ObjectSelectionModel
case class UniformSelection extends ObjectSelectionModel

object BehaviorConfig {
    def apply(requestType: RequestType,
        delayModel: RealDistributionConfiguration,
        objectSelectioModel: ObjectSelectionModel) =
        new BehaviorConfig(requestType, delayModel, objectSelectioModel)
}

class BehaviorConfig(
    val requestType: RequestType,
    val delayModel: RealDistributionConfiguration,
    val objectSelectionModel: ObjectSelectionModel)

trait StorageSimConfig {
    var simDuration: Double = 10.0

    var cloudCount: Int = 5
    var storageDevicesPerCloud: Int = 10

    var userCount: Int = 500
    var bucketCountDistribution: IntegerDistributionConfiguration = PoissonDist(20)
    var objectCountDistribution: IntegerDistributionConfiguration = PoissonDist(100)
    var objectSizeDistribution: RealDistributionConfiguration = ExponentialDist(5 * Units.MByte)

    var behaviors: Seq[BehaviorConfig] = Seq(BehaviorConfig(RequestType.Get, NormalDist(1.0, 0.3), UniformSelection()))
}