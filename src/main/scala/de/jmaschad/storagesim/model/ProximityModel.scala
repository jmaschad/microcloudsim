package de.jmaschad.storagesim.model

import de.jmaschad.storagesim.model.user.User
import scala.util.Random
import org.cloudbus.cloudsim.NetworkTopology
import java.awt.geom.Point2D
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import de.jmaschad.storagesim.model.distributor.Placement

object ProximityModel {
    def selectLowestDistance(cloudSets: Set[Set[Int]], loads: Map[User, Double]): Set[Int] = {
        val weightedMeans = getWeightedMeans(loads map {
            case (user, load) =>
                NetworkTopology.getPosition(user.netID) -> load
        })
        val placementLocations = {
            cloudSets map { cloudIDs =>
                cloudIDs -> { cloudIDs map { cid => NetworkTopology.getPosition(Entity.entityForId(cid).netID) } }
            } toMap
        }
        minDistancePlacement(placementLocations, weightedMeans)
    }

    def selectLowestDistance(placements: SortedSet[Placement], loads: Map[User, Double]): Placement = {
        val placementLocations = placements map { p =>
            p -> { p.clouds map { cid => NetworkTopology.getPosition(Entity.entityForId(cid).netID) } }
        } toMap

        val weightedMeans = getWeightedMeans(loads map { case (user, load) => NetworkTopology.getPosition(user.netID) -> load })

        minDistancePlacement(placementLocations, weightedMeans)
    }

    private def getWeightedMeans(loads: Map[Point2D, Double]): Set[Point2D] =
        loads.size match {
            case n if n <= 3 =>
                loads.keySet

            case n =>
                val clusters = kMeans(loads.keySet.toIndexedSeq)

                val meanLoad = loads.values.sum / loads.size
                val normalizedLoads = loads mapValues { _ / meanLoad }
                clusters map { cluster =>
                    val sum = cluster.foldLeft(new Point2D.Double(0.0, 0.0)) {
                        case (mean, clusterMember) =>
                            val load = normalizedLoads(clusterMember)
                            val x = mean.getX() + (load * clusterMember.getX())
                            val y = mean.getY() + (load * clusterMember.getY())
                            new Point2D.Double(x, y)
                    }
                    new Point2D.Double(sum.getX() / cluster.size, sum.getY() / cluster.size)
                } toSet
        }

    private def minDistancePlacement[T](placementLocations: Map[T, Set[Point2D]], weightedMeans: Set[Point2D]): T = {
        val min = placementLocations minBy {
            case (_, placementPositions) => placementDistance(placementPositions, weightedMeans)
        }
        min._1
    }

    private def placementDistance(placementPositions: Set[Point2D], weightedMeans: Set[Point2D]): Double = {
        // for each placement point get the distance to the nearest weighted mean
        val minDistances = placementPositions map { placementPoint =>
            weightedMeans map { placementPoint.distance(_) } min
        }

        // discard the longest distances in case there are fewer weighted means than placement locations
        { minDistances.toIndexedSeq.sorted take weightedMeans.size } sum
    }

    private def kMeans(positions: IndexedSeq[Point2D]): Iterable[IndexedSeq[Point2D]] = {
        var means = Random.shuffle(positions) take 3
        var clusters = Iterable.empty[IndexedSeq[Point2D]]
        var meansChanged = true
        while (meansChanged) {
            clusters = assignClusters(means, positions)
            val newMeans = updateMeans(clusters)
            meansChanged = checkChanged(means, newMeans)
            means = newMeans
        }
        clusters
    }

    private def assignClusters(means: IndexedSeq[Point2D], positions: IndexedSeq[Point2D]): Iterable[IndexedSeq[Point2D]] =
        positions.groupBy { position =>
            means.indexOf({
                means.sortWith { (m1, m2) =>
                    val dist1 = m1.distance(position)
                    val dist2 = m2.distance(position)
                    dist1 < dist2
                } head
            })
        } values

    private def updateMeans(clusters: Iterable[IndexedSeq[Point2D]]): IndexedSeq[Point2D] =
        clusters map { cluster =>
            val sumPos = cluster.foldLeft(new Point2D.Double(0.0, 0.0)) {
                case (sum, pos) =>
                    new Point2D.Double(sum.getX() + pos.getX(), sum.getY() + pos.getY())
            }
            new Point2D.Double(sumPos.getX() / cluster.size, sumPos.getY() / cluster.size)
        } toIndexedSeq

    private def checkChanged(m1: IndexedSeq[Point2D], m2: IndexedSeq[Point2D]): Boolean =
        { m1 zip m2 } forall { case (p1, p2) => p1.distance(p2) < 1.0 }
}