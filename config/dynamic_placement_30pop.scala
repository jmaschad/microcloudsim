import de.jmaschad.storagesim._

new StorageSimConfig {
	outputDir = "experiments/dynamic_placement_30pop"
    selector = DynamicPlacementBased()
    objectPopularityModel = ExponentialDist(0.3)
}
