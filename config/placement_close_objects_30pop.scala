import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/dynamic_placement_close_objects_30pop"
    selector = PlacementBased()
    closePlacement = true
    objectPopularityModel = ExponentialDist(0.3)
}
