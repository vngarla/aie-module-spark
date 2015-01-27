package com.attivio.platform.modules.spark

import com.attivio.model.document.AttivioDocument



class FieldConverter(fn:String, mv: Boolean) extends Serializable {
  val fieldName = fn
  val multivalued = mv

  /**
   * TODO - how to strongly type arrays?
   * @param doc
   * @return
   */
  def getFieldValue(doc: AttivioDocument):Any = {
    if(doc == null)
      return null
    if(multivalued) {
      val fvals = doc.getFieldValues(fieldName)
      if(fvals == null)
        return null
      else
        return fvals.toArray
    } else
      return doc.getFirstValue(fieldName)
  }
}

