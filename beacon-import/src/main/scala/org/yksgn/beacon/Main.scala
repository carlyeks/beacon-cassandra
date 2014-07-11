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
import org.apache.spark.Logging
import org.bdgenomics.adam.predicates.ADAMPredicate
import org.bdgenomics.adam.projections.ADAMRecordField._
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.ADAMRecord

object Main extends App with Logging {
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

    val cassandraHosts = System.getenv("CASSANDRA_HOST") match {
      case null => Seq("127.0.0.1")
      case x => x.split(",").toSeq
    }

    val sc = ADAMContext.createSparkContext("beacon: import", master, null, sparkJars, Seq())
    val proj = Projection(contig, start, sequence, readMapped, primaryAlignment, readPaired, firstOfPair)
    
    sc.union(args.map(sc.adamLoad[ADAMRecord, ADAMPredicate[ADAMRecord]](_, projection=Some(proj))))
      .filter(ar => ar.getReadMapped && (!ar.getReadPaired || ar.getFirstOfPair) && ar.primaryAlignment)
      .flatMap(ar => ar.getSequence.zipWithIndex.map{ case (s, idx) => (ar.contig.contigName, ar.start + idx, s) })
      .foreachPartition(partition => {
        val cluster = Cluster.builder()       
          .addContactPoints(cassandraHosts: _*)
          .build()
        val session = cluster.connect()
        val begin = "BEGIN UNLOGGED BATCH\n"
        val end = "APPLY BATCH;\n"

        try {
          var query = begin
          var count = 0
          partition
            .foreach({ case (ref, loc, base) => {
              if (query.length + end.length > 5120) {
                query = query + end
                session.execute(query)
                query = begin
              }
              query = query +
                "INSERT INTO beacon.locations (referenceName, location, base) VALUES ('%s', %d,'%s');\n".format(ref, loc, base)
              count = count + 1
              if (count % 100000 == 0) {
                logWarning("Wrote %d records".format(count))
              }
            }})
        } finally {
          session.close()
          cluster.close()
        }
      })
  }
}
