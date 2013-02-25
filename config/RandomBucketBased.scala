import de.jmaschad.storagesim._
import de.jmaschad.storagesim.model.user.RequestType._

new StorageSimConfig {
    passCount = 5
    selector = RandomBucketBased()
}
