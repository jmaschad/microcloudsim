package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.predicates.PredicateType
import de.jmaschad.storagesim.model.processing.Download
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.Upload
import de.jmaschad.storagesim.model.processing.Dialog
import de.jmaschad.storagesim.model.processing.Message
import de.jmaschad.storagesim.model.processing.Message
import de.jmaschad.storagesim.model.processing.Timeout

object ProcessingEntity {
    private val Base = 30000
    val DialogMessage = Base + 1
    val DialogTimeout = DialogMessage + 1
}

abstract class ProcessingEntity(
    name: String,
    resources: ResourceCharacteristics) extends SimEntity(name) {

    protected var storageSystem = new StorageSystem(log _, resources.storageDevices)
    protected var processing = new ProcessingModel(log _, scheduleProcessingUpdate _, resources.bandwidth)
    protected var downloader = new Download(send _, log _, getId)
    protected var uploader = new Upload(send _, log _, getId)

    private var dialogs = Map.empty[String, Dialog]

    private var lastUpdate: Option[SimEvent] = None

    override def startEntity(): Unit = {}

    override def shutdownEntity(): Unit

    final override def processEvent(event: SimEvent): Unit = event.getTag match {
        case ProcessingEntity.DialogMessage =>
            val message = event.getData() match {
                case m: Message => m
                case _ => throw new IllegalStateException
            }
            processDialogMessage(event.getSource(), message)

        case ProcessingEntity.DialogTimeout =>
            val timeout = event.getData match {
                case t: Timeout => t
                case _ => throw new IllegalStateException
            }
            processTimeout(timeout)

        case ProcessingModel.ProcUpdate =>
            processing.update()

        case Download.Download =>
            downloader.process(event.getSource(), event.getData())

        case Upload.Upload =>
            uploader.process(event.getSource, event.getData)

        case _ =>
            process(event)
    }

    protected def log(message: String): Unit

    protected def process(event: SimEvent): Unit

    protected def answerDialog(source: Int, message: AnyRef): Option[Dialog]

    protected def addDialog(dialog: Dialog) = dialogs += dialog.dialogId -> dialog

    protected def removeDialog(dialogId: String) = {
        assert(dialogs.isDefinedAt(dialogId))
        dialogs -= dialogId
    }

    protected def resetModel() = {
        uploader = uploader.reset()
        downloader = downloader.reset()
        processing = processing.reset()
        storageSystem = storageSystem.reset()
    }

    private def processDialogMessage(source: Int, message: Message) =
        dialogs.get(message.dialogId) match {
            case Some(dialog) if message.messageId == dialog.messageId =>
                dialog.processMessage(message.content)

            case Some(dialog) =>
                throw new IllegalStateException("received message for timed out dialog")

            case None if message.messageId == 0 =>
                answerDialog(source, message.content).
                    foreach(dialog => dialogs += dialog.dialogId -> dialog)

            case _ =>
                throw new IllegalStateException
        }

    private def processTimeout(timeout: Timeout) =
        dialogs.get(timeout.dialogId).foreach(dialog => {
            if (timeout.isTimeout(dialog.messageId)) {
                dialog.onTimeout()
                dialogs -= timeout.dialogId
            }
        })

    private def scheduleProcessingUpdate(delay: Double) = {
        lastUpdate.foreach(CloudSim.cancel(_))
        lastUpdate = Some(send(getId(), delay, ProcessingModel.ProcUpdate))
    }
}