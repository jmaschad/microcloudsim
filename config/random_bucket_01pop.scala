import de.jmaschad.storagesim._

new StorageSimConfig {
    outputDir = "experiments/random_bucket_01pop"
    selector = RandomBucketBased()
    objectPopularityModel = ExponentialDist(0.01)
}
