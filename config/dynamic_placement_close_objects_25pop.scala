import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/dynamic_placement_close_objects_25pop"
    selector = DynamicPlacementBased()
    closePlacement = true
    objectPopularityModel = ExponentialDist(0.25)
}
