import de.jmaschad.storagesim._

new StorageSimConfig {
  simDuration = 20
  userCount = 100
  cloudCount = 100
  cloudBandwidthDistribution = NormalDist(125 * Units.MByte, 20 * Units.MByte);
  objectSizeDistribution = ExponentialDist(1 * Units.MByte)
  cloudFailureDistribution = NormalDist(100 * 60, 1)
  diskFailureDistribution = NormalDist(100 * 60, 1)
}
