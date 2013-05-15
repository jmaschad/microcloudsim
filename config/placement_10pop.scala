import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/placement_10pop"
    selector = PlacementBased()
    objectPopularityModel = ExponentialDist(0.10)
}
