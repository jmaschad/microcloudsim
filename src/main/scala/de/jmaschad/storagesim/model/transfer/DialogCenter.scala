package de.jmaschad.storagesim.model.transfer

import scala.util.Random
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.ProcessingEntity

object DialogCenter {
    val Timeout = 2.0
    type TimeoutHandler = Long => Boolean
    type MessageHandler = (Message, Dialog) => Unit
}
import DialogCenter._

class DialogCenter(
    entity: ProcessingEntity,
    handlerFactory: (Int, Message) => Option[MessageHandler],
    send: (Int, Double, Int, Object) => SimEvent) {

    private var dialogs = Map.empty[String, Dialog]

    def openDialog(target: Int, messageHandler: MessageHandler): Dialog = {
        val dialog = new Dialog(target, messageHandler, this)
        assert(!dialogs.isDefinedAt(dialog.id))
        dialogs += dialog.id -> dialog
        dialog
    }

    def closeDialog(dialog: Dialog) = {
        assert(dialogs.isDefinedAt(dialog.id))
        dialogs -= dialog.id
    }

    def say(message: AnyRef, timeoutHandler: TimeoutHandler, dialog: Dialog): Unit = {
        send(dialog.partner, 0.0, ProcessingEntity.DialogMessage, new Message(dialog.messageId, dialog.id, message))
        send(entity.getId(), DialogCenter.Timeout, ProcessingEntity.DialogTimeout, new Timeout(dialog.id, timeoutHandler))
    }

    def handleMessage(source: Int, message: Message) = dialogs.get(message.dialog) match {
        // message for existing dialog
        case Some(dialog) =>
            assert(message.id == dialog.messageId)
            dialog.messageId += 1
            dialog.messageHandler(message, dialog)

        // first message of new dialog
        case None if message.id == 0 =>
            handlerFactory(source, message).foreach(handler => {
                answerDialog(source, message, handler)
            })

        case _ =>
            throw new IllegalStateException
    }

    def handleTimeout(timeout: Timeout) =
        if (timeout.handler(dialogs(timeout.dialog).messageId)) {
            dialogs -= timeout.dialog
        }

    private def answerDialog(
        source: Int,
        message: Message,
        messageHandler: MessageHandler): Unit =
        {
            val dialog = new Dialog(source, messageHandler, this, message.dialog)
            assert(!dialogs.isDefinedAt(message.dialog))
            dialogs += dialog.id -> dialog

            handleMessage(source, message)
        }
}

class Dialog(
    val partner: Int,
    var messageHandler: MessageHandler,
    val dialogCenter: DialogCenter,
    val id: String = hashCode() + "-" + Random.nextInt) {
    var messageId = 0L

    def say(message: AnyRef, timeoutHandler: TimeoutHandler) =
        dialogCenter.say(message, timeoutHandler, this)

    def close() = dialogCenter.closeDialog(this)
}

class Message(val id: Long, val dialog: String, val content: AnyRef)
class Timeout(val dialog: String, val handler: Long => Boolean)