package de.jmaschad.storagesim

import org.apache.commons.math3.distribution.IntegerDistribution
import org.apache.commons.math3.distribution.RealDistribution
import de.jmaschad.storagesim.model.user.UserBehavior
import de.jmaschad.storagesim.model.user.RequestType._
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.apache.commons.math3.distribution.ExponentialDistribution
import org.apache.commons.math3.distribution.PoissonDistribution

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
    var simDuration: Double = 30.0
    var replicaCount: Int = 3

    var cloudCount: Int = 10
    var storageDevicesPerCloud: Int = 10
    var cloudFailureDistribution: RealDistributionConfiguration = NormalDist(5 * 60, 2 * 60)
    var cloudRepairDistribution: RealDistributionConfiguration = NormalDist(30, 5)
    var diskFailureDistribution: RealDistributionConfiguration = NormalDist(60, 10)
    var diskRepairDistribution: RealDistributionConfiguration = NormalDist(15, 3)

    var userCount: Int = 1000
    var bucketCountDistribution: IntegerDistributionConfiguration = PoissonDist(20)
    var objectCountDistribution: IntegerDistributionConfiguration = PoissonDist(100)
    var objectSizeDistribution: RealDistributionConfiguration = ExponentialDist(5 * Units.MByte)

    var behaviors: Seq[BehaviorConfig] = Seq(BehaviorConfig(Get, NormalDist(1.0, 0.3), UniformSelection()))
}