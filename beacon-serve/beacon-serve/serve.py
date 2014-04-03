from flask import Flask
from cassandra.cluster import Cluster,NoHostAvailable
app = Flask(__name__)

nodes = ["localhost"]
cluster = Cluster(nodes)
session = None
query = None

@app.route("/heartbeat")
def heartbeat():
    return "OK"

@app.route("/")
def query():
    if session is None:
        raise CassandraConnectionError()
    else:
        rows = session.execute(query, ["chr1", 1, "A"])
        for row in rows:
            return "YES"
        return "NO"

if __name__ == "__main__":
    try:
        # Need to use some configuration to get the Cassandra nodes we should use
        session = cluster.connect("beacon")
        query = session.prepare("SELECT referenceName, position, base FROM beacon.locations WHERE referenceName = ? AND position = ? AND base = ?")
    except NoHostAvailable as nhe:
        print nhe
    app.run()
