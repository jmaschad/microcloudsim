import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/random_object_25pop"
    selector = RandomObjectBased()
    objectPopularityModel = ExponentialDist(0.25)
}