import de.jmaschad.storagesim._

new StorageSimConfig {
  simDuration = 20
  userCount = 100
  cloudCount = 100
  objectSizeDistribution = ExponentialDist(0.25 * Units.MByte)
  cloudFailureDistribution = NormalDist(100 * 60, 1)
  diskFailureDistribution = NormalDist(100 * 60, 1)
}
