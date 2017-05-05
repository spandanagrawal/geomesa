/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.utils

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, Point}
import org.apache.commons.codec.binary.Base64
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.conf.QueryHints.{ENCODE_STATS, RichHints, STATS_STRING}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap}
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.{Stat, StatSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.Expression

import scala.collection.mutable

trait KryoLazyDensityUtils {

  import KryoLazyDensityUtils._
  var geomIndex: Int = -1
  // we snap each point into a pixel and aggregate based on that
  var gridSnap: GridSnap = null
  var writeGeom: (SimpleFeature, DensityResult) => Unit = null

  def initialize(options: Map[String, String], sft: SimpleFeatureType): DensityResult = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    geomIndex = sft.getGeomIndex
    gridSnap = {
      val bounds = options(ENVELOPE_OPT).split(",").map(_.toDouble)
      val envelope = new Envelope(bounds(0), bounds(1), bounds(2), bounds(3))
      val Array(width, height) = options(GRID_OPT).split(",").map(_.toInt)
      new GridSnap(envelope, width, height)
    }

    // function to get the weight from the feature - defaults to 1.0 unless an attribute/exp is specified
    val weightIndex = options.get(WEIGHT_OPT).map(sft.indexOf).getOrElse(-2)
    val weightFn: (SimpleFeature) => Double =
      if (weightIndex == -2) {
        (_) => 1.0
      } else if (weightIndex == -1) {
        val expression = ECQL.toExpression(options(WEIGHT_OPT))
        getWeightFromExpression(expression)
      } else if (sft.getDescriptor(weightIndex).getType.getBinding == classOf[java.lang.Double]) {
        getWeightFromDouble(weightIndex)
      } else {
        getWeightFromNonDouble(weightIndex)
      }

    writeGeom = if (sft.isPoints) {
      (sf, result) => writePoint(sf, weightFn(sf), result)
    } else {
      (sf, result) => writeNonPoint(sf.getDefaultGeometry.asInstanceOf[Geometry], weightFn(sf), result)
    }

    mutable.Map.empty[(Int, Int), Double]
  }

  /**
    * Gets the weight for a feature from a double attribute
    */
  def getWeightFromDouble(i: Int)(sf: SimpleFeature): Double = {
    val d = sf.getAttribute(i).asInstanceOf[java.lang.Double]
    if (d == null) 0.0 else d
  }

  /**
    * Tries to convert a non-double attribute into a double
    */
  def getWeightFromNonDouble(i: Int)(sf: SimpleFeature): Double = {
    val d = sf.getAttribute(i)
    if (d == null) {
      0.0
    } else {
      val converted = Converters.convert(d, classOf[java.lang.Double])
      if (converted == null) 1.0 else converted
    }
  }

  /**
    * Evaluates an arbitrary expression against the simple feature to return a weight
    */
  def getWeightFromExpression(e: Expression)(sf: SimpleFeature): Double = {
    val d = e.evaluate(sf, classOf[java.lang.Double])
    if (d == null) 0.0 else d
  }

  /**
    * Writes a density record from a feature that has a point geometry
    */
  def writePoint(sf: SimpleFeature, weight: Double, result: DensityResult): Unit =
    writePointToResult(sf.getAttribute(geomIndex).asInstanceOf[Point], weight, result)

  /**
    * Writes a density record from a feature that has an arbitrary geometry
    */
  def writeNonPoint(geom: Geometry, weight: Double, result: DensityResult): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    writePointToResult(geom.safeCentroid(), weight, result)
  }

  protected def writePointToResult(pt: Point, weight: Double, result: DensityResult): Unit =
    writeSnappedPoint((gridSnap.i(pt.getX), gridSnap.j(pt.getY)), weight, result)

  protected def writePointToResult(pt: Coordinate, weight: Double, result: DensityResult): Unit =
    writeSnappedPoint((gridSnap.i(pt.x), gridSnap.j(pt.y)), weight, result)

  protected def writePointToResult(x: Double, y: Double, weight: Double, result: DensityResult): Unit =
    writeSnappedPoint((gridSnap.i(x), gridSnap.j(y)), weight, result)

  private def writeSnappedPoint(xy: (Int, Int), weight: Double, result: DensityResult): Unit =
    result.update(xy, result.getOrElse(xy, 0.0) + weight)

}

object KryoLazyDensityUtils {

  type DensityResult = mutable.Map[(Int, Int), Double]
  type GridIterator = (SimpleFeature) => Iterator[(Double, Double, Double)]

  val DENSITY_SFT: SimpleFeatureType = SimpleFeatureTypes.createType("density", "result:String,*geom:Point:srid=4326")
  val density_serializer: KryoFeatureSerializer  = new KryoFeatureSerializer(DENSITY_SFT, SerializationOptions.withoutId)

  // configuration keys
  val ENVELOPE_OPT = "envelope"
  val GRID_OPT     = "grid"
  val WEIGHT_OPT   = "weight"

  /**
    * Encodes a sparse matrix into a byte array
    */
  def encodeResult(result: mutable.Map[(Int, Int), Double]): Array[Byte] = {
    val output = KryoFeatureSerializer.getOutput(null)
    result.toList.groupBy(_._1._1).foreach { case (row, cols) =>
      output.writeInt(row, true)
      output.writeInt(cols.size, true)
      cols.foreach { case (xy, weight) =>
        output.writeInt(xy._2, true)
        output.writeDouble(weight)
      }
    }
    output.toBytes
  }

  /**
    * Returns a mapping of simple features (returned from a density query) to weighted points in the
    * form of (x, y, weight)
    */
  def decodeResult(envelope: Envelope, gridWidth: Int, gridHeight: Int): GridIterator =
    decodeResult(new GridSnap(envelope, gridWidth, gridHeight))

  /**
    * Decodes a result feature into an iterator of (x, y, weight)
    */
  def decodeResult(gridSnap: GridSnap)(sf: SimpleFeature): Iterator[(Double, Double, Double)] = {
    val result = sf.getAttribute(0).asInstanceOf[Array[Byte]]
    val input = KryoFeatureSerializer.getInput(result, 0, result.length)
    new Iterator[(Double, Double, Double)]() {
      private var x = 0.0
      private var colCount = 0
      override def hasNext = input.position < input.limit
      override def next() = {
        if (colCount == 0) {
          x = gridSnap.x(input.readInt(true))
          colCount = input.readInt(true)
        }
        val y = gridSnap.y(input.readInt(true))
        val weight = input.readDouble()
        colCount -= 1
        (x, y, weight)
      }
    }
  }
}

object KryoLazyStatsUtils {
  val StatsSft = SimpleFeatureTypes.createType("stats:stats", "stats:String,geom:Geometry")

  /**
    * Encodes a stat as a base64 string.
    *
    * Creates a new serializer each time, so don't call repeatedly.
    *
    * @param stat stat to encode
    * @param sft simple feature type of underlying schema
    * @return base64 string
    */
  def encodeStat(stat: Stat, sft: SimpleFeatureType): String =
    Base64.encodeBase64URLSafeString(StatSerializer(sft).serialize(stat))

  /**
    * Decodes a stat string from a result simple feature.
    *
    * Creates a new serializer each time, so not used internally.
    *
    * @param encoded encoded string
    * @param sft simple feature type of the underlying schema
    * @return stat
    */
  def decodeStat(encoded: String, sft: SimpleFeatureType): Stat =
    StatSerializer(sft).deserialize(Base64.decodeBase64(encoded))

  /**
    * Reduces computed simple features which contain stat information into one on the client
    *
    * @param features iterator of features received per tablet server from query
    * @param hints query hints that the stats are being run against
    * @return aggregated iterator of features
    */
  def reduceFeatures(sft: SimpleFeatureType, hints: Hints)
                    (features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    val transform = hints.getTransform
    val serializer = transform match {
      case Some((tdef, tsft)) => StatSerializer(tsft)
      case None               => StatSerializer(sft)
    }

    val decodedStats = features.map { f =>
      serializer.deserialize(Base64.decodeBase64(f.getAttribute(0).toString))
    }

    val sum = if (decodedStats.isEmpty) {
      // create empty stat based on the original input so that we always return something
      Stat(sft, hints.get(STATS_STRING).asInstanceOf[String])
    } else {
      val sum = decodedStats.next()
      decodedStats.foreach(sum += _)
      sum
    }
    decodedStats.close()

    val stats = if (hints.containsKey(ENCODE_STATS) && hints.get(ENCODE_STATS).asInstanceOf[Boolean]) encodeStat(sum, sft) else sum.toJson
    Iterator(new ScalaSimpleFeature("stat", StatsSft, Array(stats, GeometryUtils.zeroPoint)))
  }
}