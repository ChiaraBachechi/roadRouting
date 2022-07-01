from neo4j import GraphDatabase
import folium as fo
import argparse


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_projected_graph(self,mode):
        """This method creates a new graph as a projection of the existing nodes and relations. 
           The mode parameter is set to 'r' for dual graph and 'j' for primal graph."""
        with self.driver.session() as session:
            path = session.read_transaction(self._projected_graph,mode)

    @staticmethod
    def _projected_graph(tx,mode):
        if(mode == 'r'):
            str = """
                CALL gds.graph.create.cypher(
                    "graph",
                    "MATCH (n) where n:RoadOsm RETURN id(n) AS id",
                    "MATCH (n)-[r:CONNECTED]->(m) return id(n) as source,id(m) as target,type(r) as type,r.location as location,r.junction as junction"
                )
                        """
        else:
            str = """
                CALL gds.graph.create.cypher(
                    "graph",
                    "MATCH (n) where n:Node or n:OSMWayNode RETURN id(n) AS id, n.lat AS lat, n.lon AS lon",
                    "MATCH ()-[r:ROUTE]->() with min(r.AADT) as min_AADT,max(r.AADT) as max_AADT,max(r.distance) as max_dist,min(r.distance) as min_dist MATCH (n)-[r:ROUTE]->(m) WHERE r.status = 'active' RETURN id(n) AS source, id(m) AS target, 0.5 * toFloat((r.AADT-min_AADT)/(max_AADT-min_AADT)) + 0.5 * toFloat((r.distance-min_dist)/(max_dist-min_dist)) as traffic, r.AADT as AADT, r.distance as distance, type(r) as type"
                )
                        """
        result = tx.run(str)
        return result

    def delete_projected_graph(self):
        """This method deletes an existing graph projection. 
           The mode parameter is set to 'r' for dual graph and 'j' for primal graph."""
        with self.driver.session() as session:
            path = session.read_transaction(self._drop_projected_graph)

    @staticmethod
    def _drop_projected_graph(tx):
        result = tx.run("""
                CALL gds.graph.drop('graph')
                        """)
        return result

    def countNodes(self,mode):
        """the method counts the number of nodes of mode label"""
        with self.driver.session() as session:
            result = session.write_transaction(self._countNodes,mode)
            print('{} is the number of nodes'.format(result[0][0]))
            return result[0][0]

    @staticmethod
    def _countNodes(tx,mode):
        if( mode == 'r'):
            str = """match(n:RoadOsm) return count(n)"""
        else:
            str = """match(n:Node) return count(n)"""
        result = tx.run(str)
        return result.values()

    def countRoutes(self,mode):
        """the methods counts the number of relationships of mode type"""
        with self.driver.session() as session:
            result = session.write_transaction(self._countRoutes,mode)
            print('{} is the number of relations.'.format(result[0][0]))
            return result[0][0]

    @staticmethod
    def _countRoutes(tx,mode):
        if( mode == 'r'):
            str = """match ()-[r:CONNECTED]->() return count(*)"""
        else:
            str = """match (:Node)-[r:ROUTE]->(:Node) return count(*)"""
        result = tx.run(str)
        return result.values()

    def outgoingDegree(self):
        """the method counts the number of outgoing relationships from each node"""
        with self.driver.session() as session:
            result = session.write_transaction(self._outgoingDegree)
        return result
    @staticmethod
    def _outgoingDegree(tx):
        result = tx.run("""CALL gds.degree.stream(
        'graph',
        { orientation: 'REVERSE' }
        )
        YIELD nodeId, score
        RETURN round(avg(score),2) AS outgoing""")
        print('{} is the average outgoing degree'.format(result.values()[0][0]))
        return result

    def incomingDegree(self):
        """the method counts the number of incoming relationships from each node"""
        with self.driver.session() as session:
            result = session.write_transaction(self._incomingDegree)
        return result
    @staticmethod
    def _incomingDegree(tx):
        result = tx.run("""
        CALL gds.degree.stream(
        'graph'
        )
        YIELD nodeId, score
        RETURN round(avg(score),2) AS incoming""")
        print("{} is the average incoming degree of nodes".format(result.values()[0][0]))
        return result

    def undirectedDegree(self):
        """the method counts the number of incoming and outgoing relationships from each node"""
        with self.driver.session() as session:
            result = session.write_transaction(self._undirectedDegree)
        return result
    @staticmethod
    def _undirectedDegree(tx):
        result = tx.run("""
        CALL gds.degree.stream(
        'graph',
        { orientation: 'undirected' }
        )
        YIELD nodeId, score
        RETURN round(avg(score),2) AS undirected""")
        print("{} is the average undirected degree of nodes".format(result.values()[0][0]))
        return result

    def summarize(self):
        """the method counts the total number of relationships, the total number of nodes and the density of the graph."""
        with self.driver.session() as session:
            result = session.write_transaction(self._summarize)
        return result
    @staticmethod
    def _summarize(tx):
        result = tx.run("""
        CALL gds.graph.list()
        YIELD graphName, nodeCount, relationshipCount,density
        RETURN graphName, nodeCount, relationshipCount,round(density,6)""")
        print("The density of the graph is {}.".format(result.values()[0][3]))
        return result


def addOptions():
    parser = argparse.ArgumentParser(description='Caracteristics of the graph')
    
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    return parser


def main(args=None):
    argParser = addOptions()
    #retrieving arguments
    options = argParser.parse_args(args=args)
    #connecting with the neo4j instance
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    #asking the user if he wants to analyse the primal or dual graph
    mode = input('Select the graph you want to analyse [r] for dual graph, [j] for primal graph.')
    mode = mode.lower()
    #creating the projection of the selected graph
    greeter.create_projected_graph(mode)
    #counting the nodes
    greeter.countNodes(mode)
    #counting relationships
    greeter.countRoutes(mode)
    #evaluating the incoming degree
    greeter.incomingDegree()
    #evaluating the outgoing degree
    greeter.outgoingDegree()
    #evaluating the total degree
    greeter.undirectedDegree()
    #retrieving additional statistics (density)
    greeter.summarize()
    #removing the projected graph
    greeter.delete_projected_graph()
    greeter.close()
    return 0


main()
