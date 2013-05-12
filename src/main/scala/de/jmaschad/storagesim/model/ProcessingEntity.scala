package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Units

trait ProcessingEntity extends Entity {
    var processingModel = ProcessingModel.createModel(this)

    def bandwidth: Double

    def download(id: String, size: Double, onFinish: () => Unit) =
        processingModel.download(id, size, onFinish)

    def upload(id: String, size: Double, onFinish: () => Unit) =
        processingModel.upload(id, size, onFinish)

    abstract override protected def reset() = {
        ProcessingModel.destroyModel(this)
        processingModel = ProcessingModel.createModel(this)
        super.reset()
    }
}