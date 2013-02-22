package de.jmaschad.storagesim.model.user

import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.IntegerDistribution
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog

object RequestType extends Enumeration {
    type RequestType = Value
    val Get = Value("GET")
    val Delete = Value("DELETE")
}

object UserBehavior {
    def apply(delayModel: RealDistribution,
        objectSelectionModel: IntegerDistribution,
        objects: IndexedSeq[StorageObject],
        generator: StorageObject => RestDialog) =
        new ConfigurableBehavior(delayModel,
            objectSelectionModel,
            objects: IndexedSeq[StorageObject],
            generator: StorageObject => RestDialog)
}

trait UserBehavior {
    def timeToNextEvent(): Double
    def nextRequest(): RestDialog
}

private[user] class ConfigurableBehavior(
    delayModel: RealDistribution,
    objectSelectionModel: IntegerDistribution,
    objects: IndexedSeq[StorageObject],
    generator: StorageObject => RestDialog) extends UserBehavior {

    override def timeToNextEvent(): Double = delayModel.sample()

    override def nextRequest(): RestDialog = generator(objects(objectSelectionModel.sample()))
}

