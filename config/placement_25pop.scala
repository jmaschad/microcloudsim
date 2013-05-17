import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/placement_25pop"
    selector = PlacementBased()
    objectPopularityModel = ExponentialDist(0.25)
}
