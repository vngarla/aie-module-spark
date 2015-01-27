package com.attivio.platform.modules.spark

import com.attivio.model.Point
import com.attivio.model.document.AttivioDocument

/**
 *
 * @param fn fieldName
 * @param x extract X (true) or Y (false)
 * @param mv extract single value (false) or Array[Double] (true)
 */
class PointConverter(fn:String, mv:Boolean, x:Boolean) extends FieldConverter(fn, mv) {
  def extractCoord(pt: Point): Double = {
    if(x) return pt.getX else return pt.getY
  }

  override def getFieldValue(doc: AttivioDocument):Any = {
    if(multivalued) {
      val fvals = doc.getFieldValues(fieldName)
      if(fvals != null)
        return fvals.toArray(Array[Point]()).map(extractCoord)
      else
        return null
    } else {
      val fval = doc.getFirstValue(fieldName)
      if(fval != null)
        return extractCoord(fval.asInstanceOf[Point])
      else
        return null
    }

  }
}
