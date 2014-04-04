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
  override def main(args: Array[String]) {
    val master = System.getenv("MASTER") match {
      case null => "local[4]"
      case x => x
    }
    val sparkJars = 
      classOf[ADAMRecord].getProtectionDomain().getCodeSource().getLocation().getPath() +:
    (System.getenv("SPARK_JARS") match {
      case null => Seq()
      case x => x.split(",").toSeq
    })
    val sc = AdamContext.createSparkContext("beacon: import", master, null, sparkJars, Seq())
    val proj = Projection(referenceName, referenceUrl, start, sequence, readMapped, primaryAlignment, readPaired, firstOfPair)
    sc.union(args.map(sc.adamLoad[ADAMRecord, UnboundRecordFilter](_, projection=Some(proj))))
      .filter(ar => ar.getReadMapped && (!ar.getReadPaired || ar.getFirstOfPair) && ar.primaryAlignment)
      .flatMap(ar => ar.getSequence.zipWithIndex.map{ case (s, idx) => (ar.getReferenceName, ar.getStart + idx, s) })
      .distinct()
      .foreachPartition(partition => {
      val cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .build()
      val session = cluster.connect()

      try {
          partition
            .grouped(1000)
            .foreach(group => {
              session.execute("BEGIN UNLOGGED BATCH\n" +
                group.map { case (ref, loc, base) =>
                  "INSERT INTO beacon.locations (referenceName, location, base) VALUES ('%s', %d,'%s');\n".format(ref, loc,base)
                }.aggregate("")((p,n) => p + n, (l,r) => l + r) +
                "APPLY BATCH;\n")
            })
      } finally {
        session.close()
        cluster.close()
      }
    })
 }
}
