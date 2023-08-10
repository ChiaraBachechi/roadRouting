from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time
import folium as fo
import osmnx as ox
from shapely import wkt
import pandas as pd
import geopandas as gpd
import numpy as np
from ast import literal_eval
"""In this file we perform routing on projections using A*"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        """gets the path of the neo4j instance"""
        with self.driver.session() as session:
            result = session.write_transaction(self._get_path)
            return result

    @staticmethod
    def _get_path(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.neo4j_home' return value;
                    """)
        return result.values()

    def create_projections(self,mode,weight):
        """Create projections, one considering as weight the travel time and one the cost"""
        with self.driver.session() as session:
            result = session.write_transaction(self._create_projections,mode,weight)
            return result

    @staticmethod
    def _create_projections(tx,mode,weight):
        if(mode == 'cycleways'):
            name = 'bike_routes'
            if(weight == 'cost' or weight == 'both'):
                result = tx.run("""
                call gds.graph.create('bike_routes_cost', ['BikeCross', 'FootCross', 'JunctionBikeCross', 'JunctionFootCross', 'RoadBikeJunction', 'RoadFootJunction'], 
                ['BIKE_ROUTE', 'FOOT_ROUTE', 'IS_THE_SAME'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'cost'});
                """)
            if(weight == 'travel_time' or weight == 'both'):
                result = tx.run("""
                call gds.graph.create('bike_routes_travel_time', ['BikeCross', 'FootCross', 'JunctionBikeCross', 'JunctionFootCross', 'RoadBikeJunction', 'RoadFootJunction'], 
                ['BIKE_ROUTE', 'FOOT_ROUTE', 'IS_THE_SAME'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'travel_time'});
                """)
        elif(mode == 'footways'):
            name = 'foot_routes'
            if(weight == 'cost' or weight == 'both'):
                result = tx.run("""
                call gds.graph.create('foot_routes_cost', ['FootCross', 'JunctionFootCross', 'RoadFootJunction'], 
                ['FOOT_ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'cost'});
                """)
            if(weight == 'travel_time' or weight == 'both'):
                result = tx.run("""
                call gds.graph.create('foot_routes_travel_time', ['FootCross', 'JunctionFootCross', 'RoadFootJunction'], 
                ['FOOT_ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: 'travel_time'});
                """)
        return name
    
    def delete_projected_graph(self,weight,graph_name):
        """This method deletes an existing graph projection. 
           The mode parameter is set to 'r' for dual graph and 'j' for primal graph."""
        with self.driver.session() as session:
            path = session.read_transaction(self._drop_projected_graph,weight,graph_name)

    @staticmethod
    def _drop_projected_graph(tx,weight,graph_name):
        if weight == 'both':
            name = graph_name + '_cost'
            tx.run("""
                CALL gds.graph.drop( $name)
                        """,name = name)
            name = graph_name + '_travel_time'
            result = tx.run("""
                CALL gds.graph.drop( $name)
                        """,name = name)
        else:
            name = graph_name + '_' + weight
            result = tx.run("""
                CALL gds.graph.drop( $name)
                        """,name = name)
        return result
    
    def update_cost(tx,beta=0.5):
        with self.driver.session() as session:
            path = session.read_transaction(self._update_cost,beta)
            
    @staticmethod
    def _update_cost(tx,beta):
        tx.run("""
                MATCH(n1)-[r:BIKE_ROUTE]-(n2) with max(r.travel_time) as max_travel_time,max(r.danger) as max_danger, min(r.travel_time) as min_travel_time, min(r.danger) as min_danger 
                MATCH(n3)-[r1:BIKE_ROUTE]-(n4) with max_travel_time, min_travel_time,max_danger,min_danger, r1 
                set r1.cost = $beta *(r1.travel_time-min_travel_time)/(max_travel_time-min_travel_time) + (1-$beta2) *(r1.danger-min_danger)/(max_danger-min_danger)
                """,beta = beta, beta2 = beta)

        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]-(n2) with max(r.travel_time) as max_travel_time,max(r.danger) as max_danger, min(r.travel_time) as min_travel_time, min(r.danger) as min_danger
                MATCH(n3)-[r1:FOOT_ROUTE]-(n4) with max_travel_time, min_travel_time,max_danger,min_danger, r1 
                set r1.cost = $beta *(r1.travel_time-min_travel_time)/(max_travel_time-min_travel_time) + (1-$beta2) *(r1.danger-min_danger)/(max_danger-min_danger)
                """,beta = beta, beta2 = beta)
        return

    def get_import_folder_name(self):
        """gets the path of the import folder of the neo4j instance"""
        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()
        
    def get_the_nearest_junction_to_POI(self, osmid, foot = False):
        """Routing considering as weight the cost, which is a tradeoff between the travel time and
           the safety of the path
        """
        with self.driver.session() as session:
            print(dest)
            result = session.write_transaction(self._get_the_nearest_junction_to_POI, osmid, foot)
            return result

    @staticmethod
    def _get_the_nearest_junction_to_POI(tx, osmid, foot):
        if foot:
            result = tx.run("""match (p:PointOfInterest {osm_id: $osmid}) 
                                match (p)-[:MEMBER]->(o:OSMWayNode)
                                match (o)<-[:IS_NEAR_TO]-(m) with point({latitude: tofloat(o.lat), longitude: tofloat(o.lon)}) as location,m
                                match (m)-[:CONTAINS]->(j:Junction)
                                where j:FootCross or j:JunctionFootCross or j:RoadFootJunction
                                return distance(location,j.location) as distance, j.id
                                order by distance limit 1""",osmid = osmid)
        else:
            result = tx.run("""match (p:PointOfInterest {osm_id: $osmid}) 
                                match (p)-[:MEMBER]->(o:OSMWayNode)
                                match (o)<-[:IS_NEAR_TO]-(m) with point({latitude: tofloat(o.lat), longitude: tofloat(o.lon)}) as location,m
                                match (m)-[:CONTAINS]->(j:Junction)
                                return distance(location,j.location) as distance, j.id
                                order by distance limit 1""",osmid = osmid)
        return result.values()
        
    def get_the_nearest_junction_to_coordinates(self, lat, lon, foot = False):
        """Routing considering as weight the cost, which is a tradeoff between the travel time and
           the safety of the path
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._get_the_nearest_junction_to_coordinates, lat, lon, foot)
            return result

    @staticmethod
    def _get_the_nearest_junction_to_coordinates(tx, lat, lon, foot):
        if foot:
            print(lat)
            print(lon)
            query = """CALL spatial.withinDistance('spatial', {latitude:"""+str(lat) +""", longitude:"""+str(lon)+"""}, 0.1) 
            yield node,distance 
            where node:FootCross or node:JunctionFootCross or node:RoadFootJunction
            return distance,node.id as osmid order by distance limit 1"""
            result = tx.run(query)
        else:
            print(lat)
            print(lon)
            query = """CALL spatial.withinDistance('spatial', {latitude:"""+str(lat) +""", longitude:"""+str(lon)+"""}, 0.1) 
            yield node,distance 
            where node:Junction
            return distance,node.id as osmid order by distance limit 1"""
            result = tx.run(query)
        return result.values()


    def routing_algorithm(self, source, target, projection, mode,alg = 'd'):
        """Routing considering as weight just the travel time"""
        with self.driver.session() as session:
            result = session.write_transaction(self._routing_algorithm, source, target, projection, mode,alg)
            return result

    
    @staticmethod
    def _routing_algorithm(tx, source, target, projection, mode, alg):
        if(alg =='d'):
            print('algorithm is:')
            print(alg)
            result = tx.run("""
                    match (source:Junction {id: $source})
                    match (target:Junction {id: $target})
                    CALL gds.shortestPath.dijkstra.stream($projection, {
                    sourceNode: source,
                    targetNode: target,
                    relationshipWeightProperty: $mode
                    })
                    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                    with index,
                        totalCost,
                        [nodeId IN nodeIds | {latitude:gds.util.asNode(nodeId).lat,longitude:gds.util.asNode(nodeId).lon}] AS nodeCord,
                        [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodeIDs
                    match (n)-[:CONTAINS]->(j:Junction) where j.id in nodeIDs with collect(distinct(n.id_num)) as path, nodeIDs, totalCost, nodeCord 
                    return path, nodeCord, totalCost, nodeIDs
                    """, source=source, target=target, projection=projection, mode=mode)
        else:
            print('algorithm is:-----------------------------------')
            print(alg)
            result = tx.run("""
                    match (source:Junction {id: $source})
                    match (target:Junction {id: $target})
                    CALL gds.shortestPath.astar.stream($projection, {
                    sourceNode: source,
                    targetNode: target,
                    latitudeProperty: 'lat',
                    longitudeProperty: 'lon',
                    relationshipWeightProperty: $mode
                    })
                    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
                    with index,
                        totalCost,
                        [nodeId IN nodeIds | {latitude:gds.util.asNode(nodeId).lat,longitude:gds.util.asNode(nodeId).lon}] AS nodeCord,
                        [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodeIDs
                    match(n)-[:CONTAINS]->(j:Junction) where j.id in nodeIDs with collect(distinct(n.id_num)) as path, nodeIDs, totalCost, nodeCord 
                    return path, nodeCord, totalCost, nodeIDs
                    """, source=source, target=target, projection=projection, mode=mode)
        return result.values()
        
    def evaluate_path(self, start, end):
        """getting information about the trip"""
        with self.driver.session() as session:
            result = session.write_transaction(self._evaluate_path, start, end)
            return result
        
    @staticmethod
    def _evaluate_path(tx, start, end):
        result = tx.run("""match (n {id: $start})-[r]->(m {id: $end}) return r.length,r.danger,r.speed
        """,start = start, end = end)
        return result.values()
    
    def generate_inter_community_graph(self):
        """evaluate the best route between the source and the target
        """
        with self.driver.session() as session:
            result = session.execute_write(self._generate_inter_community_graph)
            return result
    @staticmethod
    def _generate_inter_community_graph(tx):
        query = """
        call gds.graph.project('community_graph', ['Community'], 
                ['INTRA_COMMUNITY'], 
                { relationshipProperties: ['cost']});"""
        result = tx.run(query)
        return result.values()
        
    def generate_inter_community_path(self,source,target):
        """evaluate the best route between the source and the target
        """
        with self.driver.session() as session:
            result = session.execute_write(self._generate_inter_community_path,source,target)
            return result
    @staticmethod
    def _generate_inter_community_path(tx,source,target): 
        #query = """
        #match (bks:BikeNode{id:'%s'})
        #match (bkt:BikeNode{id:'%s'})
        #match (source:Community{id:bks.louvain})
        #match (target:Community{id:bkt.louvain})
        #with source as s, target as t
        #match p = shortestPath((s)-[:INTRA_COMMUNITY*]->(t)) unwind relationships(p) as n with startNode(n).id as start_node,endNode(n).id as end_node
        #match (bk:BikeNode{louvain:start_node,border_lmatch (bks:BikeNode{id:'%ouvain:True})-[r:BORDER_LOUVAIN_ROUTE]->(bk2:BikeNode{louvain:start_node,border_louvain:True})-[r2:BIKE_ROUTE]->(bk3:BikeNode{louvain:end_node,border_louvain:True})
        #return bk.id,r.cost,r.path_cost,bk2.id,r2.cost,bk3.id,start_node,end_node"""%(source,target)
        query = """
        match (bks:BikeNode{id:'%s'})
        match (bkt:BikeNode{id:'%s'})
        match (source:Community{id:bks.louvain})
        match (target:Community{id:bkt.louvain})
        with source as s, target as t
        CALL gds.shortestPath.dijkstra.stream('community_graph', {
                                            sourceNode: s,
                                            targetNode: t,
                                            relationshipWeightProperty: 'cost'
                                            })
                                            YIELD index, sourceNode, targetNode, totalCost, nodeIds, path as p
        unwind relationships(p) as n with startNode(n).id as start_node,endNode(n).id as end_node
        match (bk:BikeNode{louvain:start_node,border_louvain:True})-[r:BORDER_LOUVAIN_ROUTE]->(bk2:BikeNode{louvain:start_node,border_louvain:True})-[r2:BIKE_ROUTE]->(bk3:BikeNode{louvain:end_node,border_louvain:True})
        return bk.id,r.cost,r.path_cost,bk2.id,r2.cost,bk3.id,start_node,end_node"""%(source,target)
        #print(query)
        result = tx.run(query)
        return result.values()
        
    def evaluate_path_metrics(self,pairs):
        """evaluate path metrics
        """
        with self.driver.session() as session:
            result = session.execute_write(self._evaluate_path_metrics,pairs)
            return result
    @staticmethod
    def _evaluate_path_metrics(tx,pairs):
        query = """unwind %s as pairs
                match (n:BikeNode{id: pairs[0]})-[r:BIKE_ROUTE]->(m:BikeNode{id:pairs[1]})
                with min(r.cost) as min_cost, pairs
                match (n:BikeNode{id: pairs[0]})-[r:BIKE_ROUTE]->(m:BikeNode{id:pairs[1]})
                where r.cost = min_cost
                return sum(r.cost) as cost,avg(r.danger) as danger,sum(r.distance) as distance"""%(pairs)
        result = tx.run(query)
        return result.values()
        
    def count_crossings(self,pairs):
        """evaluate path metrics
        """
        with self.driver.session() as session:
            result = session.execute_write(self._count_crossings,pairs)
            return result
    @staticmethod
    def _count_crossings(tx,pairs):
        query = """unwind %s as pairs
                match (n:BikeCrossing{id: pairs[0]})-[r:BIKE_ROUTE]->(m:BikeCrossing{id:pairs[1]})
                return count(n)"""%(pairs)
        result = tx.run(query)
        return result.values()
        
    def count_communities(self,pairs):
        """evaluate path metrics
        """
        with self.driver.session() as session:
            result = session.execute_write(self._count_communities,pairs)
            return result
    @staticmethod
    def _count_communities(tx,pairs):
        query = """unwind %s as pairs
                match (n:BikeNode{id: pairs[0]})-[r:BIKE_ROUTE]->(m:BikeNode{id:pairs[1]})
                where n.louvain <> m.louvain
                return count(r)"""%(pairs)
        result = tx.run(query)
        return result.values()
        
    def generate_louvain_community_graph(self, id_community):
        """generate the graph projection for the given community"""

        with self.driver.session() as session:
            result = session.execute_write(self._generate_louvain_community_graph, id_community)
            return result
    @staticmethod
    def _generate_louvain_community_graph(tx, id_community):
        query ="""call gds.graph.project.cypher('subgraph_community_lp_""" + str(id_community) + """','MATCH (n:BikeNode {louvain:"""
        query = query + str(id_community) + """}) RETURN id(n) AS id','MATCH (n:BikeNode{louvain:"""+ str(id_community) 
        query = query + """})-[r:BIKE_ROUTE]->(m:BikeNode{louvain: """ + str(id_community) + """}) RETURN id(n) AS source, id(m) AS target, r.travel_time as travel_time, r.danger as danger, r.cost as cost')
                        YIELD
                          graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels"""
        result = tx.run(query)
        return result.values()
        
    def routing_source_targets(self,source,targets):
        """evaluate the best route between the source and the target
        """
        with self.driver.session() as session:
            result = session.execute_write(self._routing_source_targets,source,targets)
            return result
    @staticmethod
    def _routing_source_targets(tx,source,targets):
        query = """
        match (s:BikeNode {id: '%s'})
        unwind %s as t_id
        match (t:BikeNode {id: t_id}) 
        with s,t
        CALL gds.shortestPath.dijkstra.stream('subgraph_community_lp_' + toString(s.louvain), {
                                            sourceNode: s,
                                            targetNode: t,
                                            relationshipWeightProperty: 'cost'
                                            })
                                            YIELD index, sourceNode, targetNode, totalCost, nodeIds
        with  gds.util.asNode(sourceNode).id as sourceNode,gds.util.asNode(targetNode).id as targetNode,[nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodes_path, totalCost as weight
        return sourceNode,targetNode,nodes_path,weight"""%(source,targets)
        result = tx.run(query)
        return result.values()
        
    def routing_sources_target(self,sources,target):
        """evaluate the best route between the source and the target
        """
        with self.driver.session() as session:
            result = session.execute_write(self._routing_sources_target,sources,target)
            return result
    @staticmethod
    def _routing_sources_target(tx,sources,target):
        query = """
        match (t:BikeNode {id: '%s'})
        unwind %s as s_id
        match (s:BikeNode {id: s_id}) 
        with s,t
        CALL gds.shortestPath.dijkstra.stream('subgraph_community_lp_' + toString(s.louvain), {
                                            sourceNode: s,
                                            targetNode: t,
                                            relationshipWeightProperty: 'cost'
                                            })
                                            YIELD index, sourceNode, targetNode, totalCost, nodeIds
        with  gds.util.asNode(sourceNode).id as sourceNode,gds.util.asNode(targetNode).id as targetNode,[nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodes_path, totalCost as weight
        return sourceNode,targetNode,nodes_path,weight"""%(target, sources)
        #print(query)
        result = tx.run(query)
        return result.values()
        
    def get_coordinates(self,final_path):
        """evaluate the best route between the source and the target
        """
        with self.driver.session() as session:
            result = session.execute_write(self._get_coordinates,final_path)
            return result
    @staticmethod
    def _get_coordinates(tx,final_path): 
        query = """
        unwind %s as p
        match (n:BikeNode{id: p}) return collect([n.lat,n.lon])"""%(final_path)
        #print(query)
        result = tx.run(query)
        return result.values()
        
    def drop_all_projections(self):
        with self.driver.session() as session:
            result = session.execute_write(self._drop_all_projections)
            return result
    @staticmethod    
    def _drop_all_projections(tx):
        result = tx.run("""CALL gds.graph.list() YIELD graphName
                    CALL gds.graph.drop(graphName)
                    YIELD database
                    RETURN 'dropped ' + graphName""")
        return result.values()

    def routing_old_style(self,source,target):
        """evaluate the best route between the source and the target
        """
        with self.driver.session() as session:
            result = session.execute_write(self._routing_old_style,source,target)
            return result
    @staticmethod
    def _routing_old_style(tx,source,target):
        tx.run("""call gds.graph.project('subgraph_routing', ['BikeJunction','BikeCrossing'], 
                ['BIKE_ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: ['cost']});
            """)
        query = """
        match (s:BikeNode {id: '%s'})
        match (t:BikeNode {id: '%s'})
        CALL gds.shortestPath.dijkstra.stream('subgraph_routing', {
                                            sourceNode: s,
                                            targetNode: t,
                                            relationshipWeightProperty: 'cost'
                                            })
                                            YIELD index, sourceNode, targetNode, totalCost, nodeIds, path
        with  [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodes_path, totalCost as weight,path as p
        unwind relationships(p) as n with startNode(n).id as start_node,endNode(n).id as end_node,nodes_path,weight
        match (bk:BikeNode{id:start_node})-[r:BIKE_ROUTE]->(bk2:BikeNode{id:end_node})
        return nodes_path,weight, sum(r.danger) as total_danger, sum(r.distance) as total_distance"""%(source,target)
        result = tx.run(query)
        tx.run("""call gds.graph.drop('subgraph_routing')""")
        return result.values()[0]
        
        
        
def routing_with_communities(greeter,source,target,boolMap=False,file=''):
    greeter.generate_inter_community_graph()
    #find all the shortest path between communities and then find all the possible border nodes path
    start_time = time.time()
    result = greeter.generate_inter_community_path(source,target)
    percorso = pd.DataFrame(result, columns = ['bk.id', 'r.cost', 'r.path_cost', 'bk2.id', 'r2.cost', 'bk3.id',
       'start_node', 'end_node'])
    percorso['r.path_cost'] = percorso['r.path_cost'].apply(str)
    #get the sequence of communities
    sequence = percorso['start_node'].unique()
    sequence = np.append(sequence,percorso['end_node'].iloc[-1])
    #find all the possible paths and their costs
    choice=pd.DataFrame()
    for i in range(0,len(sequence)-1):
        if i==0:
            if len(sequence)>2:
                choice = pd.merge(percorso[(percorso['start_node']==sequence[i]) & (percorso['end_node']==sequence[i+1])], percorso[(percorso['start_node']==sequence[i+1]) & (percorso['end_node']==sequence[i+2])], 
                                  left_on=  ['bk3.id'],
                           right_on= ['bk.id'], 
                           how = 'outer',
                            suffixes = ['_' + str(i),'_' + str(i+1)]).dropna()
            else:
                choice = percorso[(percorso['start_node']==sequence[i])].rename(
                                  columns ={'bk.id': 'bk.id_'+ str(i),'r.cost':'r.cost_'+ str(i),'r.path_cost':'r.path_cost_'+ str(i),'bk2.id':'bk2.id_'+ str(i),'r2.cost':'r2.cost_'+ str(i),'bk3.id':'bk3.id_'+ str(i),'start_node':'start_node_'+ str(i),'end_node':'end_node_'+ str(i)})
        elif i == len(sequence)-2:
            choice = pd.merge(choice,
                              percorso[(percorso['start_node']==sequence[i])].rename(
                                  columns ={'bk.id': 'bk.id_'+ str(i),'r.cost':'r.cost_'+ str(i),'r.path_cost':'r.path_cost_'+ str(i),'bk2.id':'bk2.id_'+ str(i),'r2.cost':'r2.cost_'+ str(i),'bk3.id':'bk3.id_'+ str(i),'start_node':'start_node_'+ str(i),'end_node':'end_node_'+ str(i)}),
                              on = ['bk.id_' + str(i),'r.cost_' + str(i),'r.path_cost_' + str(i),'bk2.id_' + str(i),'r2.cost_' + str(i),'bk3.id_' + str(i),'start_node_' + str(i),'end_node_' + str(i)])
        else:
            choice = pd.merge(choice,
                            pd.merge(percorso[percorso['start_node']==sequence[i]], 
                                         percorso[percorso['start_node']==sequence[i+1]],
                                         left_on=  ['bk3.id'],right_on= ['bk.id'],
                                         how = 'outer',
                                    suffixes = ['_' + str(i),'_' + str(i+1)]).dropna(),
                              on = ['bk.id_' + str(i),'r.cost_' + str(i),'r.path_cost_' + str(i),'bk2.id_' + str(i),'r2.cost_' + str(i),'bk3.id_' + str(i),'start_node_' + str(i),'end_node_' + str(i)]
                             )
            choice['min_cost'] = choice['r.cost_' + str(i)] + choice['r2.cost_' + str(i)] + choice['r.cost_' + str(i+1)] + + choice['r2.cost_' + str(i+1)]
            choice = choice.loc[choice.groupby(by=['bk.id_' + str(i),'bk3.id_' + str(i+1)]).min_cost.idxmin()]
            choice = choice.drop(['min_cost'], axis=1)
    #get the total cost of the path between the communities
    choice['total_cost'] = 0
    for i in range(0,len(sequence)-1):
        if i != 0:
            choice['total_cost'] = choice['r.cost_' + str(i)] + choice['r2.cost_' + str(i)] + choice['total_cost']
        else:
            choice['total_cost'] = choice['r2.cost_' + str(i)] + choice['total_cost']
    #generate the graph of the source community
    greeter.generate_louvain_community_graph(sequence[0])   
    #generate all the possible path between the soruce node and the border nodes that point to the next community
    str_target = '[' + ",".join('"'  + str(x) + '"' for x in choice['bk2.id_0'].unique()) + ']'
    df_source = pd.DataFrame(greeter.routing_source_targets(source,str_target), columns = ['source', 'border', 'r.path_cost_s', 'cost'])
    df_source['r.path_cost_s'] = df_source['r.path_cost_s'].apply(str)
    choice = pd.merge(df_source,choice,left_on=  ['border'],right_on= ['bk2.id_0'],how='outer')
    #generate the graph of the target community
    greeter.generate_louvain_community_graph(sequence[-1])
    #generate all the possible path between the target node and the border nodes that comes from the previous community
    str_source = '[' + ",".join('"'  + str(x) + '"' for x in choice['bk3.id_'+ str(len(sequence)-2)].unique()) + ']'
    df_target = pd.DataFrame(greeter.routing_sources_target(str_source,target), columns = ['border', 'target', 'r.path_cost_t', 'cost_t']) 
    choice = pd.merge(df_target,choice,left_on=  ['border'],right_on= ['bk3.id_'+ str(len(sequence)-2)],how='outer',
                      suffixes = ['_target',''])
    #find the total cost of the path
    choice['total_cost'] = choice['total_cost'] + choice['cost'] + choice['cost_t']
    path = []
    for x in literal_eval(choice.sort_values(by=['total_cost']).iloc[0,:]['r.path_cost_s']):
            path.append(x)
    for i in range(1,len(sequence)-1):
        for x in literal_eval(choice.sort_values(by=['total_cost']).iloc[0,:]['r.path_cost_' + str(i)]):
            if x != path[-1:]:
                path.append(x)
        if i == len(sequence)-2:
            path.append(choice.sort_values(by=['total_cost']).iloc[0,:]['bk3.id_'+str(i)].replace('"',''))
    for x in choice.sort_values(by=['total_cost']).iloc[0,:]['r.path_cost_t']:
        if x != path[-1:]:
            path.append(x)
    dic= {}
    dic['exec_time']=time.time() - start_time
    dic['hops']=len(path)
    #visualization of the path
    if (boolMap):
        coordinates = greeter.get_coordinates(final_path = str(path))
        k = fo.Map(location=[coordinates[0][0][0][0], coordinates[0][0][0][1]], zoom_start=13)
        if len(coordinates[0][0]) == 0:
                print('\nNo result for query')
        else:
            fo.PolyLine(coordinates[0][0], color="red", weight=5).add_to(k)
            k.save(file +'.html')
    #evaluation of the path
    pairs = []
    for i in range(0,len(path)-1):
        pairs.append([path[i],path[i+1]])
    ev = greeter.evaluate_path_metrics(pairs = str(pairs))
    dic['source'] = source
    dic['target'] = target
    dic['cost'] = ev[0][0]
    dic['danger']= ev[0][1]
    dic['distance']= ev[0][2]
    dic['#crossings']= greeter.count_crossings(pairs = str(pairs))[0][0]
    dic['#communities']= greeter.count_communities(pairs = str(pairs))[0][0]
    #dic['pairs'] = pairs
    greeter.drop_all_projections()
    return dic

def routing_old_way(greeter,source,target,boolMap=False,file=''):
    start_time = time.time()
    result = greeter.routing_old_style(source,target)
    path = result[0]
    cost = result[1]
    final_path = []

    for p in path:
        if final_path:
            if str(p) != final_path[-1:][0]:
                final_path.append(str(p))
        else:
            final_path.append(str(p))
    dic= {}
    dic['exec_time']=time.time() - start_time
    dic['hops']=len(final_path)
    if (boolMap):
        #visualization of the path
        coordinates = greeter.get_coordinates(final_path = str(final_path))
        m = fo.Map(location=[coordinates[0][0][0][0], coordinates[0][0][0][1]], zoom_start=13)
        if len(coordinates[0][0]) == 0:
                print('\nNo result for query')
        else:
            fo.PolyLine(coordinates[0][0], color="green", weight=5).add_to(m)
            m.save(file + '.html')
    #evaluation of the path
    pairs = []
    for i in range(0,len(final_path)-1):
        pairs.append([final_path[i],final_path[i+1]])
    ev = greeter.evaluate_path_metrics(pairs = str(pairs))
    dic['source'] = source
    dic['target'] = target
    dic['cost'] = ev[0][0]
    dic['danger']= ev[0][1]
    dic['distance']= ev[0][2]
    dic['#crossings']= greeter.count_crossings(pairs = str(pairs))[0][0]
    dic['#communities']= greeter.count_communities(pairs = str(pairs))[0][0]
    greeter.drop_all_projections()
    return dic

def read_file(path):
    """Read the file at the specified path"""
    f = open(path)
    fjson = json.load(f)
    df = pd.DataFrame(fjson['data'])
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs='epsg:3035')
    gdf.drop('index', axis=1, inplace=True)
    return gdf


def replace_ids(l):
    """Replace the subgraph's nodes id"""
    for el in l:
        if 'roadbike/' in el:
            l[l.index(el)] = int(el.replace('roadbike/', ''))
        elif 'junctionbike/' in el:
            l[l.index(el)] = int(el.replace('junctionbike/', ''))
        elif 'junctionfoot/' in el:
            l[l.index(el)] = int(el.replace('junctionfoot/', ''))    
        elif 'bike/' in el:
            l[l.index(el)] = int(el.replace('bike/', ''))
        elif 'roadfoot/' in el:
            l[l.index(el)] = int(el.replace('roadfoot/', ''))
        elif 'foot/' in el:
            l[l.index(el)] = int(el.replace('foot/', ''))
        else:
            l[l.index(el)] = int(el)

    return l


def creation_map(result_routing_cost, mapName):
    """Draw the map with the obtained paths"""
    locations = result_routing_cost[0][1]

    coordinates_cost = []

    for x in locations:
        coordinates_cost.append((x['latitude'], x['longitude']))

    m3 = folium.Map([locations[0]['latitude'], locations[0]['longitude']], zoom_start=15)

    #geo_j = folium.GeoJson(data=gdf_cycleways,
                           #style_function=lambda x: {'fillColor': 'orange'})

    #folium.GeoJson(data=gdf_footways,
                   #style_function=lambda x: {'color': 'purple', 'weight': '3'}).add_to(geo_j)

    #geo_j.add_to(m3)

    folium.PolyLine(coordinates_cost,
                    color='red',
                    weight=5,
                    opacity=0.8).add_to(m3)

    m3.save( mapName)

def creation_map_total(result_routing_cost, result_routing_travel, mapName):
    """Draw the map with the obtained paths"""
    locations1 = result_routing_cost[0][1]
    locations2 = result_routing_travel[0][1]

    coordinates_cost = []

    for x in locations1:
        coordinates_cost.append((x['latitude'], x['longitude']))

    coordinates_travel = []

    for x in locations2:
        coordinates_travel.append((x['latitude'], x['longitude']))

    m3 = folium.Map([locations1[0]['latitude'], locations1[0]['longitude']], zoom_start=15)

    #geo_j = folium.GeoJson(data=gdf_cycleways,
                           #style_function=lambda x: {'fillColor': 'orange'})

    #folium.GeoJson(data=gdf_footways,
                   #style_function=lambda x: {'color': 'purple', 'weight': '3'}).add_to(geo_j)

    #geo_j.add_to(m3)

    folium.PolyLine(coordinates_cost,
                    color='green',
                    weight=5,
                    opacity=0.8).add_to(m3)

    folium.PolyLine(coordinates_travel,
                    color='blue',
                    weight=5,
                    opacity=0.8).add_to(m3)

    m3.save(mapName)

def add_options():
    """Parameters needed to run the script"""
    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of your starting location""",
                        required=False)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of your starting location""",
                        required=False)
    parser.add_argument('--latitude_dest', '-x_dest', dest='lat_dest', type=float,
                        help="""Insert latitude of your destination location""",
                        required=False)
    parser.add_argument('--longitude_dest', '-y_dest', dest='lon_dest', type=float,
                        help="""Insert longitude of your destination location""",
                        required=False)
    parser.add_argument('--destination', '-d', dest='dest', type=str,
                        help="""Insert the osm identifier of your destination""",
                        required=False)
    parser.add_argument('--source', '-s', dest='source', type=str,
                        help="""Insert the osm identifier of your source""",
                        required=False)
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--mode', '-m', dest='mode', type=str,
                        help="""Choose the modality of routing : cycleways, footways, community or old.""",
                        required=True)
    parser.add_argument('--alg', '-a', dest='alg', type=str,
                        help="""Choose the modality of routing : astar (a) or dijkstra (d).""",
                        required=False, default = 'd')
    parser.add_argument('--weight', '-w', dest='weight', type=str,help="""Insert the weight to use in order to perform the routing : travel_time, cost or both.""",
                        required=False, default = 'both')
    parser.add_argument('--mapName', '-mn', dest='mapName', type=str,
                        help="""Insert the name of the file containing the map with the computed path.""",
                        required=True)
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' 

    #G = ox.io.load_graphml(path + options.file_name)
    #nodes, edges = ox.graph_to_gdfs(G)
    #nodes.reset_index(inplace=True)
    #print("Loading grapml file : done")
    
    """The user can choose how to travel, using a bike or just walking"""
    graph_projection = ""
    foot = False
    if options.mode == 'cycleways':
        graph_projection = "bike_routes"
    elif options.mode == 'footway':
        graph_projection = "foot_routes"
        foot = True
    elif options.mode == 'community':
        result = routing_with_communities(greeter,options.source,options.dest,True,options.mapName)
        print("execution time:" + str(result['exec_time']))
        print("number of hops:" + str(result['hops']))
        print("total cost:" + str(result['cost']))
        print("average danger:" + str(result['danger']))
        print("total distance:" + str(result['distance']))
        print("number of crossings:" + str(result['#crossings']))
        print("number of communities:" + str(result['#communities']))
        return 0
    elif options.mode == 'old':
        result = routing_old_way(greeter,options.source,options.dest,True,options.mapName)
        print("execution time:" + str(result['exec_time']))
        print("number of hops:" + str(result['hops']))
        print("total cost:" + str(result['cost']))
        print("average danger:" + str(result['danger']))
        print("total distance:" + str(result['distance']))
        print("number of crossings:" + str(result['#crossings']))
        print("number of communities:" + str(result['#communities']))
        return 0

    if options.lat == '':
        if options.source != '':
            result = greeter.get_the_nearest_junction_to_POI(options.source,foot)
            print(result)
            distance_source = result[0][0]
            source_osmid = result[0][1]
        else:
            print("SOURCE INFORMATION ARE REQUIRED")
            raise RuntimeError("Wrong parameter value")
    else:
        if options.lon != '':
            result = greeter.get_the_nearest_junction_to_coordinates(options.lat, options.lon, foot)
            print(result)
            distance_source = result[0][0]
            source_osmid = result[0][1]
        else:
            print("SOURCE INFORMATION ARE REQUIRED")
            greeter.delete_projected_graph()
            raise RuntimeError("Wrong parameter value")
    
    if options.lat_dest == '':
        if options.dest != '':
            result = greeter.get_the_nearest_junction_to_POI(options.dest,foot)
            print(result)
            distance_dest = result[0][0]
            dest_osmid = result[0][1]
        else:
            print("DESTINATION INFORMATION ARE REQUIRED")
            raise RuntimeError("Wrong parameter value")
    else:
        if options.lon_dest != '':
            result = greeter.get_the_nearest_junction_to_coordinates(options.lat_dest, options.lon_dest, foot)
            print(result)
            distance_dest = result[0][0]
            dest_osmid = result[0][1]
        else:
            print("DESTINATION INFORMATION ARE REQUIRED")
            raise RuntimeError("Wrong parameter value")
    print(distance_source,source_osmid,distance_dest,dest_osmid)
    #create graph projections
    graph_name = greeter.create_projections(options.mode, options.weight)
    if options.weight == "cost" or options.weight == "travel_time":
        """Routing considering as weight the cost"""
        result_routing_cost = greeter.routing_algorithm(source_osmid, dest_osmid, graph_projection+"_"+options.weight,options.weight, options.alg )
        print("Find the best path between your source location and the target location, considering the travel time needed and the level of security of the paths used : done")
        print(result_routing_cost[0][1])
        listNodes = result_routing_cost[0][3]
        rels= []
        for l in range(0,len(listNodes)-1):
            rels.append((listNodes[l],listNodes[l+1]))
        length = 0
        danger = 0
        time = 0
        for r in rels:
            result = greeter.evaluate_path(r[0],r[1])
            if result[0][0]:
                length = length + float(result[0][0])
            if result[0][1]:
                danger = danger + result[0][1]
            if result[0][2]:
                speed = result[0][2]/3.6
            time = length/speed
        danger = danger / len(rels)
        """Generation of the map with the obtained paths displayed"""
        creation_map(result_routing_cost, options.mapName)
        print("Creation of the map with the two paths drawn on it : done ")
        print('cost:')
        print(result_routing_cost[0][2])
        print('number of hops:')
        print(len(result_routing_cost[0][3]))
        print('Length in meters:')
        print(length)
        print('Average danger:')
        print(danger)
        print('Total travel time in minutes:')
        print(time/60)

    elif options.weight == "both":
        """Routing considering as weight the cost"""
        result_routing_cost = greeter.routing_algorithm(source_osmid, dest_osmid, graph_projection + "_cost", "cost", options.alg )
        print(
            """Find the best path between your source location and the target location,
            considering the travel time needed and the level of security of the paths used : done""")
        listNodes = result_routing_cost[0][3]
        rels= []
        for l in range(0,len(listNodes)-1):
            rels.append((listNodes[l],listNodes[l+1]))
        length = 0
        danger = 0
        time = 0
        for r in rels:
            result = greeter.evaluate_path(r[0],r[1])
            if result[0][0]:
                length = length + float(result[0][0])
            if result[0][1]:
                danger = danger + result[0][1]
            if result[0][2]:
                speed = result[0][2]/3.6
            time = length/speed
        danger = danger / len(rels)
        print('cost:')
        print(result_routing_cost[0][2])
        print('number of hops:')
        print(len(result_routing_cost[0][3]))
        print('Length in meters:')
        print(length)
        print('Average danger:')
        print(danger)
        print('Total travel time in minutes:')
        print(time/60)
        """Routing considering as weight the travel time"""
        result_routing_travel_time = greeter.routing_algorithm(source_osmid, dest_osmid, graph_projection + "_travel_time", "travel_time", options.alg )
        print(
            "Find the best path between your source location and the target location, considering only the travel time needed : done")
        listNodes = result_routing_travel_time[0][3]
        rels= []
        for l in range(0,len(listNodes)-1):
            rels.append((listNodes[l],listNodes[l+1]))
        length = 0
        danger = 0
        time = 0
        for r in rels:
            result = greeter.evaluate_path(r[0],r[1])
            if result[0][0]:
                length = length + float(result[0][0])
            if result[0][1]:
                danger = danger + result[0][1]
            if result[0][2]:
                speed = result[0][2]/3.6
            time = length/speed
        danger = danger / len(rels)
        print('cost:')
        print(result_routing_travel_time[0][2])
        print('number of hops:')
        print(len(result_routing_travel_time[0][3]))
        print('Length in meters:')
        print(length)
        print('Average danger:')
        print(danger)
        print('Total travel time in minutes:')
        print(time/60)
        """Generation of the map with the obtained paths displayed"""
        creation_map_total(result_routing_cost, result_routing_travel_time, options.mapName)
        print("Creation of the map with the two paths drawn on it : done ")

    else:
        raise RuntimeError("Wrong parameter value")
    greeter.delete_projected_graph(options.weight, graph_name)
    return 0


if __name__ == "__main__":
    main()