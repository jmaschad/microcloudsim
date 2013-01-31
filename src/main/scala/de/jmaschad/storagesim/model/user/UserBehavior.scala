package de.jmaschad.storagesim.model.user

import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.IntegerDistribution
import de.jmaschad.storagesim.model.transfer.StorageObject

object UserBehavior {
    def apply(delayModel: RealDistribution,
        objectSelectionModel: IntegerDistribution,
        objects: IndexedSeq[StorageObject],
        generator: StorageObject => Request) =
        new ConfigurableBehavior(delayModel,
            objectSelectionModel,
            objects: IndexedSeq[StorageObject],
            generator: StorageObject => Request)
}

trait UserBehavior {
    def timeToNextEvent(): Double
    def nextRequest(): Request
}

private[user] class ConfigurableBehavior(
    delayModel: RealDistribution,
    objectSelectionModel: IntegerDistribution,
    objects: IndexedSeq[StorageObject],
    generator: StorageObject => Request) extends UserBehavior {

    override def timeToNextEvent(): Double = delayModel.sample()

    override def nextRequest(): Request = generator(objects(objectSelectionModel.sample()))
}

