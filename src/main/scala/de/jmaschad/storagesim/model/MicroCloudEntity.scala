package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.TransferModel
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.StorageObject
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.predicates.PredicateType

abstract class MicroCloudEntity(
    name: String,
    resources: ResourceCharacteristics,
    initialObjects: Iterable[StorageObject]) extends SimEntity(name) {
    protected val storageSystem = new StorageSystem(resources.storageDevices, initialObjects)
    protected val processing = new ProcessingModel(log _, scheduleProcessingUpdate _, resources.bandwidth)
    protected val transferModel = new TransferModel((target, tag, data) => sendNow(target, tag, data), this, processing)

    def startEntity(): Unit = {
        send(getId, TransferModel.TickDelay, TransferModel.Tick)
    }

    def processEvent(event: SimEvent): Unit = event.getTag match {
        case ProcessingModel.ProcUpdate =>
            log("chain update")
            processing.update()

        case TransferModel.Tick =>
            transferModel.tick()
            send(getId, TransferModel.TickDelay, TransferModel.Tick)

        case TransferModel.Transfer =>
            transferModel.process(event.getSource(), event.getData())
    }

    def shutdownEntity(): Unit

    def log(message: String): Unit

    protected def load(obj: StorageObject, target: Int, onFinish: (Boolean => Unit) = _ => {}) =
        storageSystem.loadTransaction(obj) match {
            case Some(trans) =>
                transferModel.startUpload(trans, target)
            case None => onFinish(false)
        }

    protected def store(obj: StorageObject, onFinish: (Boolean => Unit) = _ => {}) =
        storageSystem.storeTransaction(obj) match {
            case Some(trans) =>
                transferModel.expectDownload(trans)
            case None => onFinish(false)
        }

    protected def resetModel() = {
        processing.reset()
        transferModel.reset()
        storageSystem.reset()
    }

    private def scheduleProcessingUpdate(delay: Double) = {
        CloudSim.cancelAll(getId(), new PredicateType(ProcessingModel.ProcUpdate))
        send(getId(), delay, ProcessingModel.ProcUpdate)
    }

}