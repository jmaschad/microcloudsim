package de.jmaschad.storagesim.model.behavior

import de.jmaschad.storagesim.model.request.Request
import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.IntegerDistribution
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.storage.StorageObject

object Behavior {
    def apply(delayModel: RealDistribution,
        objectSelectionModel: IntegerDistribution,
        objects: IndexedSeq[StorageObject],
        generator: (StorageObject, Double) => Request) =
        new ConfigurableBehavior(delayModel,
            objectSelectionModel,
            objects: IndexedSeq[StorageObject],
            generator: (StorageObject, Double) => Request)
}

trait Behavior {
    def timeToNextEvent(): Double
    def nextRequest(time: Double): Request
}

private[behavior] class ConfigurableBehavior(
    delayModel: RealDistribution,
    objectSelectionModel: IntegerDistribution,
    objects: IndexedSeq[StorageObject],
    generator: (StorageObject, Double) => Request) extends Behavior {

    override def timeToNextEvent(): Double = delayModel.sample()

    override def nextRequest(time: Double): Request = generator(objects(objectSelectionModel.sample()), time)
}

