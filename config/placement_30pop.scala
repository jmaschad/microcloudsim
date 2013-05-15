import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/placement_30pop"
    selector = PlacementBased()
    objectPopularityModel = ExponentialDist(0.30)
}
