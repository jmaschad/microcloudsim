import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/dynamic_placement_05pop"
    selector = DynamicPlacementBased()
    objectPopularityModel = ExponentialDist(0.05)
}
