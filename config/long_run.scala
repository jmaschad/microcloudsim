import de.jmaschad.storagesim._

new StorageSimConfig {
  simDuration = 24 * 60 * 60
  cloudCount = 10
  userCount = 10000
  objectSizeDistribution = ExponentialDist(1 * Units.MByte)
}
