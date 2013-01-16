package de.jmaschad.storagesim.model.disposer

import java.util.Objects

final class ReplicationRequest(val targets: Iterable[Int], val bucket: String) {
    override def equals(other: Any) = other match {
        case that: ReplicationRequest =>
            bucket.equals(that.bucket)
        case _ => false
    }

    override def hashCode: Int = Objects.hash(bucket)
}