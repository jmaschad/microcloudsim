import de.jmaschad.storagesim._

new StorageSimConfig {
  simDuration = 5 * 60
  userCount = 20
  cloudCount = 10
  objectSizeDistribution = ExponentialDist(0.25 * Units.MByte)
}
