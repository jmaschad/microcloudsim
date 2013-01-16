package de.jmaschad.storagesim.model.behavior

import de.jmaschad.storagesim.model.user.Request
import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.IntegerDistribution
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.storage.StorageObject

object Behavior {
    def apply(delayModel: RealDistribution,
        objectSelectionModel: IntegerDistribution,
        objects: IndexedSeq[StorageObject],
        generator: StorageObject => Request) =
        new ConfigurableBehavior(delayModel,
            objectSelectionModel,
            objects: IndexedSeq[StorageObject],
            generator: StorageObject => Request)
}

trait Behavior {
    def timeToNextEvent(): Double
    def nextRequest(): Request
}

private[behavior] class ConfigurableBehavior(
    delayModel: RealDistribution,
    objectSelectionModel: IntegerDistribution,
    objects: IndexedSeq[StorageObject],
    generator: StorageObject => Request) extends Behavior {

    override def timeToNextEvent(): Double = delayModel.sample()

    override def nextRequest(): Request = generator(objects(objectSelectionModel.sample()))
}

