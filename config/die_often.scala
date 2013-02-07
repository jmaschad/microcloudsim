import de.jmaschad.storagesim._

new StorageSimConfig {
  simDuration = 60
  userCount = 10
  cloudCount = 10
  objectSizeDistribution = ExponentialDist(0.25 * Units.MByte)
  cloudFailureDistribution = NormalDist(30, 10)
  cloudRepairDistribution = NormalDist(15, 3)
}
