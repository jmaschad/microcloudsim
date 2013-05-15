import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/placement_close_objects_10pop"
    selector = PlacementBased()
    closePlacement = true
    objectPopularityModel = ExponentialDist(0.1)
}
