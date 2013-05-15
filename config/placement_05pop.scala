import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/placement_05pop"
    selector = PlacementBased()
    objectPopularityModel = ExponentialDist(0.05)
}
