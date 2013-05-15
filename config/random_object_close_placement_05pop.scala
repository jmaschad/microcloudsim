import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/random_object_close_placement_05pop"
    selector = RandomObjectBased()
    closePlacement = true
    objectPopularityModel = ExponentialDist(0.05)
}