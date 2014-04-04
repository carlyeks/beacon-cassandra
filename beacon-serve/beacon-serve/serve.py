# Copyright (C) 2014. Carl Yeksigian.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA

from flask import Flask
from cassandra.cluster import Cluster,NoHostAvailable
app = Flask(__name__)

nodes = ["localhost"]
cluster = Cluster(nodes)
session = None
query = None

@app.route("/v1/heartbeat")
def heartbeat():
    return "OK"

@app.route("/v1/<referenceName>/<int:location>/<base>")
def query(referenceName, location, base):
    if session is None:
        raise CassandraConnectionError()
    else:
        if session.execute(query, [referenceName, location, base])[0][0] == 0:
            return "NO"
        else:
            return "YES"

if __name__ == "__main__":
    try:
        # Need to use some configuration to get the Cassandra nodes we should use
        session = cluster.connect("beacon")
        query = session.prepare("SELECT COUNT(1) FROM beacon.locations WHERE referenceName = ? AND location = ? AND base = ?")
    except NoHostAvailable as nhe:
        print nhe
    app.run()
