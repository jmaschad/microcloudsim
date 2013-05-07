package de.jmaschad.storagesim.model.transfer.dialogs

import de.jmaschad.storagesim.model.StorageObject

abstract sealed class CloudStatusDialog
case class CloudOnline extends CloudStatusDialog
case class ObjectAdded(obj: StorageObject) extends CloudStatusDialog
case class CloudStatusAck extends CloudStatusDialog

