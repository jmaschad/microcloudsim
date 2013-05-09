package de.jmaschad.storagesim.model

class StorageObject(
    val name: String,
    val bucket: String,
    val size: Double,
    val popularity: Double,
    val load: Double) {
    override def toString = "StorageObject '%s/%s'".format(bucket, name)
}