# beacon-cassandra

This is a simple implementation of Beacon running directly on
Cassandra.

There are two operations:

- Import
- Serve

When importing new BAM/ADAM files, queries to the Serve operation may
be inconsistent.

The import phase goes through a BAM/ADAM file and marks all of the
chromosomes and positions which have a certain base. If there is no
entry, then the answer for Beacon should be a "NO". If there is an
entry, then the answer for Becaon should be a "YES". Because of this,
we only need to run a single query over the Cassandra cluster after
the data has been imported.

## Executing Import

The Import runs using ADAM, thus it needs to be compiled down to a
jar. The Import operation is in beacon-import/ and is maven managed.

## Executing Serve

The Serve operation is written in Python and runs using Flask and
Cassandra Driver.

    pip install -r beacon-serve/requirements.txt
    beacon-serve/bin/serve

# License

Copyright (C) 2014. Carl Yeksigian.

beacon-cassandra is made available under the GNU General Public
License (GPL) version 3 or any later version.
