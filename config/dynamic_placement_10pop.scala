import de.jmaschad.storagesim._

new StorageSimConfig {
	outputDir = "experiments/dynamic_placement_10pop"
    selector = DynamicPlacementBased()
    objectPopularityModel = ExponentialDist(0.1)
}
