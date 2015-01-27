package com.attivio.platform.modules.spark

import com.attivio.model.document.AttivioDocument
import org.apache.spark.sql.catalyst.expressions.Row

/**
 * Created by vijay on 1/27/2015.
 */
class DocToRowConverter(converters: Array[FieldConverter]) extends Serializable {
  def docToRow(doc: AttivioDocument): Row = {
    return Row(converters.map(c => c.getFieldValue(doc)):_*)
  }
}
