package de.jmaschad.storagesim.model.transfer

import scala.util.Random
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.ProcessingEntity

object DialogCenter {
    val Timeout = 2.0
    type TimeoutHandler = Long => Boolean
    type MessageHandler = Message => Unit
}
import DialogCenter._

class DialogCenter(
    entity: ProcessingEntity,
    handlerFactory: (Int, Message) => Option[MessageHandler],
    send: (Int, Double, Int, Object) => SimEvent) {

    private class Dialog(
        val partner: Int,
        val messageHandler: MessageHandler,
        val id: String = hashCode() + "-" + Random.nextInt) {
        var messageId = 0L
    }
    private var dialogs = Map.empty[String, Dialog]

    def openDialog(
        target: Int,
        messageHandler: MessageHandler): Unit =
        {
            val dialog = new Dialog(target, messageHandler)
            assert(!dialogs.isDefinedAt(dialog.id))
            dialogs += dialog.id -> dialog
        }

    def closeDialog(dialogId: String) = {
        assert(dialogs.isDefinedAt(dialogId))
        dialogs -= dialogId
    }

    def say(message: (AnyRef, TimeoutHandler), dialogId: String): Unit = {
        val dialog = dialogs(dialogId)
        send(dialog.partner, 0.0, ProcessingEntity.DialogMessage, new Message(dialog.messageId, dialog.id, message._1))
        send(entity.getId(), DialogCenter.Timeout, ProcessingEntity.DialogTimeout, new Timeout(dialog.id, message._2))
    }

    def handleMessage(source: Int, message: Message) = dialogs.get(message.dialog) match {
        // message for existing dialog
        case Some(dialog) =>
            assert(message.id == dialog.messageId)
            dialog.messageId += 1
            dialog.messageHandler(message)

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
            val dialog = new Dialog(source, messageHandler, message.dialog)
            assert(!dialogs.isDefinedAt(message.dialog))
            dialogs += dialog.id -> dialog

            handleMessage(source, message)
        }
}

class Message(val id: Long, val dialog: String, val content: AnyRef)
class Timeout(val dialog: String, val handler: Long => Boolean)