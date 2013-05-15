import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/random_object_close_placement_30pop"
    selector = RandomObjectBased()
    closePlacement = true
    objectPopularityModel = ExponentialDist(0.30)
}