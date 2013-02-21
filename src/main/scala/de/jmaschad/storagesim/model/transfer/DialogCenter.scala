package de.jmaschad.storagesim.model.transfer

import scala.util.Random
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.ProcessingEntity

object DialogCenter {
    val Timeout = 2.0
    type TimeoutHandler = () => Unit
    type MessageHandler = Message => Unit
}
import DialogCenter._

class DialogCenter(
    entity: ProcessingEntity,
    handlerFactory: (Dialog, Message) => Option[MessageHandler],
    send: (Int, Double, Int, Object) => SimEvent) {

    private var dialogs = Map.empty[String, Dialog]

    def openDialog(target: Int): Dialog = {
        val dialog = new Dialog(target, this)
        assert(!dialogs.isDefinedAt(dialog.id))
        dialogs += dialog.id -> dialog
        dialog
    }

    def closeDialog(dialog: Dialog) = {
        assert(dialogs.isDefinedAt(dialog.id))
        dialogs -= dialog.id
    }

    def say(message: AnyRef, timeoutHandler: TimeoutHandler, dialog: Dialog): Unit = {
        val init = dialog.messageId == 0
        send(dialog.partner, 0.0, ProcessingEntity.DialogMessage, new Message(dialog.id, message, init))
        send(entity.getId(), DialogCenter.Timeout, ProcessingEntity.DialogTimeout,
            new Timeout(dialog.id, dialog.messageId, timeoutHandler))
    }

    def handleMessage(source: Int, message: Message): Unit = dialogs.get(message.dialog) match {
        // message for existing dialog
        case Some(dialog) =>
            dialog.messageId += 1
            dialog.messageHandler(message)

        // first message of new dialog
        case None if message.init =>
            val dialog = answerDialog(source, message)
            handlerFactory(dialog, message) match {
                case Some(handler) =>
                    dialog.messageHandler = handler
                    handleMessage(source, message)

                case None =>
                    throw new IllegalStateException
            }

        case _ =>
            throw new IllegalStateException
    }

    def handleTimeout(timeout: Timeout) = {
        dialogs.get(timeout.dialog).foreach(dialog => {
            if (timeout.messageId == dialog.messageId) {
                timeout.handler()
                dialogs -= timeout.dialog
            }
        })
    }

    private def answerDialog(source: Int, message: Message): Dialog = {
        val dialog = new Dialog(source, this, message.dialog)

        assert(!dialogs.isDefinedAt(message.dialog))
        dialogs += dialog.id -> dialog

        dialog
    }
}

class Dialog(
    val partner: Int,
    val dialogCenter: DialogCenter,
    val id: String = hashCode() + "-" + Random.nextInt) {

    var messageHandler: MessageHandler = (_) => throw new IllegalStateException
    var messageId = 0L

    def say(message: AnyRef, timeoutHandler: TimeoutHandler) = {
        dialogCenter.say(message, timeoutHandler, this)
    }

    def close() = dialogCenter.closeDialog(this)
}

class Message(val dialog: String, val content: AnyRef, val init: Boolean)
class Timeout(val dialog: String, val messageId: Long, val handler: () => Unit)