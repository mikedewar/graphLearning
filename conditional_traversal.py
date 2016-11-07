# gremlin
from gremlin_python import statics
from gremlin_python.process.graph_traversal import __
statics.load_statics(globals())

# logging and its config
import logging
logging.basicConfig(level=logging.INFO)

# the rest
import asyncio
import json
import networkx as nx
import goblin
# this import is important until we can use the newer version of GraphSON
from goblin.driver.serializer import GraphSONMessageSerializer

# our vertices and edges
class Account(goblin.Vertex):
    pass

class Pays(goblin.Edge):
    pass

# get the loop
loop = asyncio.get_event_loop()

# hook the loop up to Gremlin, using the default titan 11 install
remote_conn = loop.run_until_complete(goblin.driver.Connection.open(
    "http://localhost:8182/gremlin", 
    loop,
    message_serializer=GraphSONMessageSerializer
))
logging.info("opened connection to gremlin")

# open up an app connected to the loop
app = loop.run_until_complete(goblin.Goblin.open(loop))

# build a gremlin driver and get the base traversal
# I'm not sure how this actually works. In the shell, we need to:
# f = TitanFactory.build().set("storage.backend","cassandra").set("storage.hostname","127.0.0.1").open();
# g = f.traversal()
# for some reason we don't have to do all this stuff with goblin. 

graph = goblin.driver.AsyncGraph()
g = graph.traversal().withRemote(remote_conn)

# this function inserts an arbitrary networkx graph. Probably rather slowly.
# this sort of thing might be better performed with titan directly?
async def insert_graph(nodes, edges, app):
    app.register(Account, Pays)
    session = await app.session()
    for acc in nodes:
        session.add(acc)
    for edge in edges:
        session.add(edge)
    logging.info("inserting synthetic graph")
    await session.flush()
    await app.close()

def add_edge(edge, nodes):
    e = Pays()
    e.source = nodes[edge[0]]
    e.target = nodes[edge[1]]
    return e

# this function, I think, somehow gives the object returned when you make a traversal
# to gremlin and stpes through it. I guess there's iterator magic going on. Pretty neat!
async def iterate_traversal(traversal):
    async for msg in traversal:
        print(json.dumps(msg, indent=1))

# when I get his working, I'll play with a big arbitrary graph. For now though,
# a little one I know the topology of.

#G = nx.scale_free_graph(3000)
G = nx.DiGraph()
G.add_edge(0,1)
G.add_edge(1,2)
G.add_edge(1,3)
G.add_edge(2,4)
G.add_edge(2,5)
G.add_edge(2,6)
G.add_edge(2,7)
G.add_edge(3,8)
G.add_edge(3,9)

nodes = [Account() for node in G.nodes()]
edges = [add_edge(edge, nodes) for edge in G.edges_iter()]

## we should clean up before this, which in the shell I would run
## g.V().drop().iterate()
## thoguh for some reason I can't figure this out from goblin

loop.run_until_complete(insert_graph(nodes, edges, app))

# it's handy to have the first node's id in case you want to play with this in the shell
seed_id = nodes[0].id
print('seed id: %s'%seed_id)

# this is what I'd like to get goblin making
'''
g.V(seed_id).repeat( out().where(out().count().is(lt(5)))).times(2).out().path()
'''

# this doesn't work
sg = g.V(seed_id).repeat(out()).times(1)

loop.run_until_complete(iterate_traversal(sg))

# don't forget to close the connection!
async def close_connection(conn):
    await conn.close()
    logging.info("closed connection")

loop.run_until_complete(close_connection(remote_conn))

# once we've disconnected the ioloop from gremlin, close it too. 
loop.close()
