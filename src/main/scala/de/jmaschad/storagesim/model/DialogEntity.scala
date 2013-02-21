package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.transfer.Timeout
import de.jmaschad.storagesim.model.transfer.Message
import de.jmaschad.storagesim.model.transfer.DialogCenter
import de.jmaschad.storagesim.model.transfer.Dialog

object DialogEntity {
    private val Base = 30000
    val DialogMessage = Base + 1
    val DialogTimeout = DialogMessage + 1
}

trait DialogEntity extends SimEntity {
    protected var dialogCenter = new DialogCenter(this, createMessageHandler _, send _)

    abstract override def processEvent(event: SimEvent): Unit = event.getTag match {
        case DialogEntity.DialogMessage if dialogsEnabled =>
            val message = event.getData() match {
                case m: Message => m
                case _ => throw new IllegalStateException
            }
            dialogCenter.handleMessage(event.getSource(), message)

        case DialogEntity.DialogTimeout =>
            val timeout = event.getData match {
                case t: Timeout => t
                case _ => throw new IllegalStateException
            }
            dialogCenter.handleTimeout(timeout)

        case _ =>
            super.processEvent(event)
    }

    protected def dialogsEnabled: Boolean = true

    protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogCenter.MessageHandler]
}