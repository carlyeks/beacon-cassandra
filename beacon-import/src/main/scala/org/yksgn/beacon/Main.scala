/**
  * Copyright (C) 2014. Carl Yeksigian.
  *
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

package org.yksgn.beacon

import com.datastax.driver.core._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField._
import edu.berkeley.cs.amplab.adam.projections.Projection
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.AdamContext
import parquet.filter.UnboundRecordFilter

object Main extends App {
  val query = "INSERT INTO beacon.locations (chromosome, location, base) VALUES (?, ?, ?)"

  override def main(args: Array[String]) {
    val master = System.getenv("MASTER") match {
      case null => "local[4]"
      case x => x
    }
    val sc = AdamContext.createSparkContext("beacon: import", master, null, Seq(), Seq())
    val proj = Projection(referenceName, referenceUrl, start, sequence, readMapped, primaryAlignment, readPaired, firstOfPair)
    sc.union(args.map(sc.adamLoad[ADAMRecord, UnboundRecordFilter](_, projection=Some(proj))))
      .filter(ar => ar.getReadMapped && (!ar.getReadPaired || ar.getFirstOfPair) && ar.primaryAlignment)
      .flatMap(ar => ar.getSequence.zipWithIndex.map{ case (s, idx) => (ar.getReferenceName, ar.getStart + idx, s) })
      .foreachPartition(partition => {
      val cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .build()
      try {
        val session = cluster.connect()

        val prepared = session.prepare(query)

        partition.foreach { case (ref, loc, base) => session.execute(prepared.bind(ref, Long.box(loc), base.toString)) }
      } finally {
        cluster.close()
      }
    })
 }
}
