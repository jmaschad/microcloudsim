import de.jmaschad.storagesim._
import de.jmaschad.storagesim.model.user.RequestType._

new StorageSimConfig {
  simDuration = 10
  userCount = 100
  cloudCount = 100
  cloudBandwidthDistribution = NormalDist(125 * Units.MByte, 0.01 * Units.MByte);
  objectSizeDistribution = ExponentialDist(5 * Units.MByte)
  cloudFailureDistribution = NormalDist(100 * 60, 1)
  diskFailureDistribution = NormalDist(100 * 60, 1)
  behaviors = Seq(BehaviorConfig(Get, NormalDist(0.5, 0.1), UniformSelection()))
}
