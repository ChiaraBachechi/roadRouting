from neo4j import GraphDatabase
import folium as fo
import pandas as pd
import os
import webbrowser
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
                    "r",
                    "MATCH (n) where n:RoadOsm RETURN id(n) as id,toInteger(round(n.traffic,0)) as traffic",
                    "MATCH (n)-[r:CONNECTED]->(m) return id(n) as source,id(m) as target,type(r) as type,r.location as location,r.junction as junction,r.score as traffic"
                )
                        """
        else:
            str = """
                CALL gds.graph.create.cypher(
                    "j",
                    "MATCH (n) where n:RoadJunction or n:OSMWayNode RETURN id(n) AS id, n.lat AS lat, n.lon AS lon",
                    "MATCH ()-[r:ROUTE]->() with min(r.AADT) as min_AADT,max(r.AADT) as max_AADT,max(r.distance) as max_dist,min(r.distance) as min_dist MATCH (n)-[r:ROUTE]->(m) WHERE r.status = 'active' RETURN id(n) AS source, id(m) AS target, toFloat(r.AADT) / toFloat(r.distance) as traffic, r.AADT as AADT, r.distance as distance, type(r) as type"
                )
                        """
        result = tx.run(str)
        return result

    def delete_projected_graph(self,mode):
        """This method deletes an existing graph projection. 
           The mode parameter is set to 'r' for dual graph and 'j' for primal graph."""
        with self.driver.session() as session:
            path = session.read_transaction(self._drop_projected_graph,mode)

    @staticmethod
    def _drop_projected_graph(tx,mode):
        result = tx.run("""
                CALL gds.graph.drop('""" + mode + """')
                        """)
        return result


    def betweenness_centrality(self):
        """This method evaluates the BC of the primal graph."""
        with self.driver.session() as session:
            path = session.write_transaction(self._betweenness_centrality)
            return path

    @staticmethod
    def _betweenness_centrality(tx):
        result = tx.run("""
                    CALL gds.betweenness.write(
                     'j',
                     {
                      writeProperty: 'bc'
                     }
                    )
                    YIELD
                    nodePropertiesWritten,
                    minimumScore,
                    maximumScore 
                        """)
        return result.values()

    def degree_centrality(self):
        """This method evaluates the Degree Centrality of the primal graph."""
        with self.driver.session() as session:
            path = session.write_transaction(self._degree_centrality)
            return path

    @staticmethod
    def _degree_centrality(tx):
        result = tx.run("""
                    CALL gds.degree.write(
                     'j',
                     {
                      writeProperty: 'degree',
                      relationshipWeightProperty: 'traffic'
                     }
                    )
                    YIELD nodePropertiesWritten, centralityDistribution
                    return centralityDistribution.min AS minimumScore,
                    centralityDistribution.max as maximumScore,nodePropertiesWritten
                        """)
        return result.values()

    def get_important_junctions(self,property):
        """This method returns the junctions with the higher centrality value.
           The property indicates the name of the centrality we are considering: bc or degree."""
        with self.driver.session() as session:
            path = session.read_transaction(self._get_important_junctions,property)
            return path

    @staticmethod
    def _get_important_junctions(tx,property):
        result = tx.run("""
                    match (n:RoadJunction) return n.id as osmid,n.lat as latitude,n.lon as longitude,n.""" + property + """ as score order by score desc""")
        df = pd.DataFrame(result.values(),columns = result.keys())
        print(df.head())
        return df

    def update_property(self):
        """This method transfers the degree centrality property from junctions in the primal graph
           to relationships in the dual graph."""
        with self.driver.session() as session:
            path = session.write_transaction(self._update_property)
            return path

    @staticmethod
    def _update_property(tx):
        result = tx.run("""
                    match (n:RoadJunction)
                    with n.id as id_junction,n.degree as degree
                    match (:RoadOsm)-[c:CONNECTED {junction: id_junction}]->(:RoadOsm)
                    set c.score = degree
                    """)
        return result.values()

    def speaker_listener_community(self):
        """This method applies Speaker Listener community detection algorithm to the dual graph."""
        with self.driver.session() as session:
            result = session.read_transaction(self._speaker_listener_community)
            return result

    @staticmethod
    def _speaker_listener_community(tx):
        result = tx.run("""CALL gds.alpha.sllpa.stream('r', {maxIterations: 100, minAssociationStrength: 0.1})
        YIELD nodeId, values
        return gds.util.asNode(nodeId).osmid AS osmid,gds.util.asNode(nodeId).name AS name, values.communityIds AS communityIds,size(values.communityIds) as dim
        ORDER BY dim DESC, name ASC""")
        df = pd.DataFrame(result.values(),columns = result.keys())
        print(df.head())
        return df

    def get_road_points(self,list):
        """This method returns the geometry of a road of the dual graph finding the corresponding nodes in the primal graph."""
        with self.driver.session() as session:
            result = session.read_transaction(self._get_road_points,list)
            return result
    
    @staticmethod
    def _get_road_points(tx,list):
        query = """with """ + str(list) + """ as list
                    unwind list as l
                    match  (n)-[:ROUTE {osmid: l}]->(m) return n.lat as lat_start,n.lon as lon_start,m.lat as lat_end,m.lon as lon_end,l as osmid
                    order by osmid,n.location"""
        print(query)
        result = tx.run(query)
        df = pd.DataFrame(result.values(),columns = result.keys())
        return df
        
    def page_rank_roads(self):
        """This method applies Page Rank algorithm to the dual graph weighted on the traffic."""
        with self.driver.session() as session:
            result = session.read_transaction(self._page_rank_roads)
            return result

    @staticmethod    
    def _page_rank_roads(tx):
        query = """CALL gds.pageRank.stream('r', {
                    dampingFactor: 0.85,
                    relationshipWeightProperty: 'traffic'
                    })
                    YIELD nodeId, score
                    return gds.util.asNode(nodeId).osmid AS osmid,gds.util.asNode(nodeId).name AS name, score
                    ORDER BY score DESC, name ASC
                """
        result = tx.run(query)
        df = pd.DataFrame(result.values(),columns = result.keys())
        return df


def addOptions():
    parser = argparse.ArgumentParser(description='Routing between two point of interest nodes in OSM.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--lat', '-x', dest='latitude', type=float,
                        help="""Insert the latitude of the central point of the generated map. SRID 4326""",
                        required=True)
    parser.add_argument('--lon', '-y', dest='longitude', type=float,
                        help="""Insert the longitude of the central point of the generated map. SRID 4326""",
                        required=True)
    parser.add_argument('--file', '-f', dest='filename', type=str,
                        help="""Insert the path where to save the node scores in a csv.""",
                        required=True)
    parser.add_argument('--action', '-a', dest='action', type=int,
                        help="""Define the action: 1 for most important junctions (BC), 2 for most cogested junctions(DC),
                              3 for most influent roads (SLLPA), 4 for most congested raods (DC+PR)""",
                        default = 0,
                        required=False)
    return parser


def main(args=None):
    argParser = addOptions()
    #reading the arguments
    options = argParser.parse_args(args=args)
    #connecting to the neo4j instance
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    #generating the folium map
    m = fo.Map(location=[options.latitude, options.longitude], zoom_start=13)
    mode = 'x'
    if(options.action == 0):
        mode = input('Do you want to visualize the most important junctions considering road network structure? y[yes],n[No]')
        mode = mode.lower()
    #visualize the most important junctions (with highest BC) considering road network structure
    if(mode=='y' or mode =='yes' or options.action == 1):    
        #create the projected primal graph
        greeter.create_projected_graph('j')
        #evaluating Betweenness centrality of the primal graph
        greeter.betweenness_centrality()
        #returning the junctions ordered by their Betweenness Centrality
        df = greeter.get_important_junctions('bc')
        #saving the results in a csv file
        df.to_csv(options.filename,index = False)
        #getting the first 100 most important junctions
        df_100 = df.head(100)
        #showing the most important junctions in a folium map
        locations = df_100[['latitude', 'longitude']]
        locationlist = locations.values.tolist()
        greeter.delete_projected_graph('j')
        for point in range(0, df_100.shape[0]):
            fo.Marker(locationlist[point], popup=df_100['osmid'][point]).add_to(m)
        #open the map in the browser
        m.save('betweenness.html')
        new = 2 # open in a new tab, if possible
        #open an HTML file on my own (Windows) computer
        url = "file://" + os.getcwd() +  '/betweenness.html'
        webbrowser.open(url,new=new)
        print('\nLook in your browser: a map with the 100 most important junctions will be displayed\n')
    if(options.action == 0):
        mode = input('Do you want to visualize the most important junctions considering traffic? y[yes],n[No]')
        mode = mode.lower()
    #visualize the most important junctions considering traffic
    if(mode=='y' or mode =='yes' or options.action == 2):
        #create the projected primal graph
        greeter.create_projected_graph('j')
        #evaluate degree centrality of junctions
        greeter.degree_centrality()
        #returning the junctions ordered by their Degree Centrality
        df = greeter.get_important_junctions('degree')
        #saving the results in a csv file
        df.to_csv(options.filename.split('.')[0]+'_AADT.csv',index = False)
        #getting the first 100 most important junctions
        df_100 = df.head(100)
        #showing the most important junctions in a folium map
        locations = df_100[['latitude', 'longitude']]
        locationlist = locations.values.tolist()
        greeter.delete_projected_graph('j')
        for point in range(0, df_100.shape[0]):
            fo.Marker(locationlist[point], popup=df_100['osmid'][point]).add_to(m)
        m.save('degree.html')
        #open the map in the browser
        new = 2 # open in a new tab, if possible
        #open an HTML file on my own (Windows) computer
        url = "file://" + os.getcwd() +  '/degree.html'
        webbrowser.open(url,new=new)
        print('\nLook in your browser: a map with the 100 most important junctions considering traffic will be displayed\n')
    if(options.action == 0): 
        mode = input('If you have generated the road section graph you can visualize the most important roads.Do you? y[yes],n[No]')
        mode = mode.lower()
    #visualize the most important roads based on Speaker Listener community detection
    if(mode=='y' or mode =='yes' or options.action == 3):
        #creating the dual graph
        greeter.create_projected_graph('r')
        #applying Speaker Listener community algorithm to the dual graph
        df = greeter.speaker_listener_community()
        greeter.delete_projected_graph('r')
        #saving results in a csv file
        df.to_csv(options.filename.split('.')[0]+'_road_community.csv',index = False)
        #returning the geometry of the roads that appears
        #in a number of community equal to the average number of community plus 1
        points = greeter.get_road_points(df[df.dim > (round(df.dim.mean(),0) + 1)].osmid.apply(str).tolist())
        print(points.head())
        #visualizing the results in a folium map
        for x in points.osmid.unique():
            point_list = []
            #print(points[points.osmid == x])
            for i,p in points[points.osmid == x][['lat_start','lon_start','lat_end','lon_end']].iterrows():
                point_list.append([p.lat_start,p.lon_start])
                point_list.append([p.lat_end,p.lon_end])
            fo.PolyLine(point_list).add_to(m)
        m.save('roads.html')
        #opening the map in the web browser
        new = 2 # open in a new tab, if possible
        #open an HTML file on my own (Windows) computer
        url = "file://" + os.getcwd() +  '/roads.html'
        webbrowser.open(url,new=new)
        print('\nLook in your browser: a map with the roads located in several community and thus influential considering the graph topology are shown.\n')
    if(options.action == 0):
        mode = input('If you have generated the road section graph and you have inserted traffic data you can visualize the most important roads considering traffic.Do you? y[yes],n[No]')
        mode = mode.lower()
    #visualize the most important roads considering traffic
    if(mode=='y' or mode =='yes' or options.action == 4):
        #creating the projections for primal graph
        greeter.create_projected_graph('j')
        #evaluating the degree centrality for primal graph
        greeter.degree_centrality()
        #transferring the degree centrality property to relations in the dual graph
        greeter.update_property()
        greeter.delete_projected_graph('j')
        #creating the projection for the dual graph
        greeter.create_projected_graph('r')
        #evaluating page rank based on the transferred degree centrality
        df = greeter.page_rank_roads()
        greeter.delete_projected_graph('r')
        #save results to csv
        df.to_csv(options.filename.split('.')[0]+'_page-rank.csv',index = False)
        #returning the geometry of the roads that have a PG >= at the average PG + 2 times the std
        points = greeter.get_road_points(df[df.score>= df.score.mean() + df.score.std()*2 ].osmid.apply(str).tolist())
        print(points.head())
        #visualizing the results in a folium map
        for x in points.osmid.unique():
            point_list = []
            #print(points[points.osmid == x])
            for i,p in points[points.osmid == x][['lat_start','lon_start','lat_end','lon_end']].iterrows():
                point_list.append([p.lat_start,p.lon_start])
                point_list.append([p.lat_end,p.lon_end])
            fo.PolyLine(point_list).add_to(m)
        m.save('roadstraffic.html')
        #visualizing the results in the web browser
        new = 2 # open in a new tab, if possible
        #open an HTML file on my own (Windows) computer
        url = "file://" + os.getcwd() +  '/roadstraffic.html'
        webbrowser.open(url,new=new)
        print('\nLook in your browser: a map with the roads with the highest page rank score considering traffic are displayed.\n')
    greeter.close()
    return 0


main()
