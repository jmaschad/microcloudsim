import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/placement_close_objects_01pop"
    selector = PlacementBased()
    closePlacement = true
    objectPopularityModel = ExponentialDist(0.01)
}
