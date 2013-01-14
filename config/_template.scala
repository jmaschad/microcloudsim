import de.jmaschad.storagesim._

new StorageSimConfig {
  simDuration = // default 10.0

  cloudCount = // default 5
  storageDevicesPerCloud = // default 10

  userCount = // default 500
  bucketCountDistribution = // default PoissonDist(20)
  objectCountDistribution = // default PoissonDist(100)
  objectSizeDistribution = // default ExponentialDist(5 * Units.MByte)

  behaviors = // default Seq(BehaviorConfig(RequestType.Get, NormalDist(1.0, 0.3), UniformSelection()))
}
