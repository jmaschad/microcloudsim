package de.jmaschad.storagesim.model.user

import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.IntegerDistribution
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.microcloud.CloudRequest

object RequestType extends Enumeration {
    type RequestType = Value
    val Get = Value("GET")
    val Delete = Value("DELETE")
}

object UserBehavior {
    def apply(delayModel: RealDistribution,
        objectSelectionModel: IntegerDistribution,
        objects: IndexedSeq[StorageObject],
        generator: StorageObject => CloudRequest) =
        new ConfigurableBehavior(delayModel,
            objectSelectionModel,
            objects: IndexedSeq[StorageObject],
            generator: StorageObject => CloudRequest)
}

trait UserBehavior {
    def timeToNextEvent(): Double
    def nextRequest(): CloudRequest
}

private[user] class ConfigurableBehavior(
    delayModel: RealDistribution,
    objectSelectionModel: IntegerDistribution,
    objects: IndexedSeq[StorageObject],
    generator: StorageObject => CloudRequest) extends UserBehavior {

    override def timeToNextEvent(): Double = delayModel.sample()

    override def nextRequest(): CloudRequest = generator(objects(objectSelectionModel.sample()))
}

