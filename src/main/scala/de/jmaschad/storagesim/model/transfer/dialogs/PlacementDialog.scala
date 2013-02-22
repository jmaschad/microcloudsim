package de.jmaschad.storagesim.model.transfer.dialogs

import de.jmaschad.storagesim.model.processing.StorageObject

abstract sealed class PlacementDialog
case class Load(objSourceMap: Map[StorageObject, Int]) extends PlacementDialog
case class PlacementAck extends PlacementDialog