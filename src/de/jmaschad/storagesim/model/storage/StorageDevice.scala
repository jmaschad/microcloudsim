package de.jmaschad.storagesim.model.storage

class StorageDevice(bandwidth: Double, capacity: Double) {
  var allocated = 0.0

  def allocate(size: Double) = {
    allocated += size
  }

  def deallocate(size: Double) = {
    allocated -= size
  }

  def hasAvailableSpace(size: Double): Boolean = (capacity - allocated) > size

}