/**
  *     Copyright (C) 2014. Carl Yeksigian.
  * 
  *     This program is free software: you can redistribute it and/or modify
  *     it under the terms of the GNU General Public License as published by
  *     the Free Software Foundation, either version 3 of the License, or
  *     (at your option) any later version.
  * 
  *     This program is distributed in the hope that it will be useful,
  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  *     GNU General Public License for more details.
  * 
  *     You should have received a copy of the GNU General Public License
  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

package org.yksgn.beakin

import com.datastax.driver.core
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField._
import edu.berkeley.cs.amplab.adam.projections.Projection
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.AdamContext
import parquet.filter.UnboundRecordFilter

object Main extends App {
  override def main(args: Array[String]) {
    val sc = AdamContext.createSparkContext("beakin: import", "local[4]", null, Seq(), Seq())
    val proj = Projection(referenceName, referenceUrl, start, sequence, readMapped, primaryAlignment, isPaired, firstOfPair)
    val allRecords = sc.union(args.map(sc.adamLoad[ADAMRecord, UnboundRecordFilter](_, projection=Some(proj))))
      .filter(ar => ar.getReadMapped && (!ar.getIsPaired || ar.getFirstOfPair) && ar.primaryAlignment)
      .flatMap(ar => ar.getSequence.zipWithIndex.map{ case (s, idx) => (ar.getReferenceName, ar.getStart + idx, s) })

    val query = "INSERT INTO beacon.locations (chromosome, location, base) VALUES (?, ?, ?)"
 }
}
