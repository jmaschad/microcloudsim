import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/dynamic_placement_close_objects_10pop"
    selector = DynamicPlacementBased()
    closePlacement = true
    objectPopularityModel = ExponentialDist(0.1)
}
