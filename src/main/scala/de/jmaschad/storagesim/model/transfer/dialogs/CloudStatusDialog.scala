package de.jmaschad.storagesim.model.transfer.dialogs

import de.jmaschad.storagesim.model.processing.StorageObject

abstract sealed class CloudStatusDialog
case class CloudOnline extends CloudStatusDialog
case class DownloadStarted(obj: StorageObject) extends CloudStatusDialog
case class DownloadFinished(obj: StorageObject) extends CloudStatusDialog
case class CloudStatusAck extends CloudStatusDialog

