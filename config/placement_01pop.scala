import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/placement_01pop"
    selector = PlacementBased()
    objectPopularityModel = ExponentialDist(0.01)
}
