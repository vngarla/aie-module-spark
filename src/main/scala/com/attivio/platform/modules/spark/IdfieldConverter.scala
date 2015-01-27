package com.attivio.platform.modules.spark

import com.attivio.model.document.AttivioDocument

/**
 * get the doc id
 */
class IdFieldConverter() extends FieldConverter(".id", false) {
  override def getFieldValue(doc: AttivioDocument):Any = {
    val id = doc.getId
    if(id != null)
      return id
    return doc.getFirstValueAsString(".id")
  }
}
