import de.jmaschad.storagesim._

new StorageSimConfig {
  simDuration = 60
  userCount = 100
  cloudCount = 10
  objectSizeDistribution = ExponentialDist(0.25 * Units.MByte)
}
