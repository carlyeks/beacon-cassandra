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
        rows = session.execute(query, [referenceName, location, base])
        for row in rows:
            return "YES"
        return "NO"

if __name__ == "__main__":
    try:
        # Need to use some configuration to get the Cassandra nodes we should use
        session = cluster.connect("beacon")
        query = session.prepare("SELECT referenceName, location, base FROM beacon.locations WHERE referenceName = ? AND location = ? AND base = ?")
    except NoHostAvailable as nhe:
        print nhe
    app.run()
