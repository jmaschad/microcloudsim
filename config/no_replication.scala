import de.jmaschad.storagesim._

new StorageSimConfig {
  simDuration = 5 * 60
  userCount = 20
  cloudCount = 5
  objectSizeDistribution = ExponentialDist(0.25 * Units.MByte)
  cloudFailureDistribution = NormalDist(100 * 60, 1)
  diskFailureDistribution = NormalDist(100 * 60, 1)
}
