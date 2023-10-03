from typing import List
from config.neo4j_helper import Neo4jHelper
from infrastructure.data.filter import Filter

class EdgeRepository:
    
    def __init__(self) -> None:
        self.neo4j_conn = Neo4jHelper(uri="neo4j://localhost:7687", user="neo4j", pwd="neo4j")
        self.intersect_attribute = ['area','areaA','areaB']
        pass

    def get_edges_by_block(self, year: int, block: int):
        query_string = f"MATCH (n:Lot{year} {{Block:\"{block}\"}})-[r:INTERSECTION]->(m) RETURN n,r,m"
        print(query_string)
        return self.neo4j_conn.query(query_string, db='neo4j')
    
    def get_edges_by_blocklist(self, year: int, initial_block: int, end_block: int, borough: str, filter_list: List[Filter]):
        block_list = range(initial_block, end_block+1)
        query_string = f"MATCH (n:Lot{year})-[r:INTERSECTION]->(m) WHERE n.Block in {[block for block in block_list]} AND n.Borough = '{borough}' "
        for filter in filter_list:
            if filter.attribute in self.intersect_attribute:
                query_string += f" AND r.{filter.attribute} {filter.operand} {filter.value}"
            elif isinstance(filter.value, (float,int)):    
                query_string += f" AND (n.{filter.attribute} {filter.operand} {filter.value} OR m.{filter.attribute} {filter.operand} {filter.value})"
            else: 
                query_string += f" AND (n.{filter.attribute} {filter.operand} '{filter.value}' OR m.{filter.attribute} {filter.operand} '{filter.value}')"
        query_string += " RETURN n,r,m"
        #print(query_string)
        return self.neo4j_conn.query(query_string, db='neo4j')

    def get_edges_by_bbl(self, bbl_list: List[int], initial_year: int, end_year: int):
        year_list = range(initial_year, end_year+1)
        query_string = f"MATCH (n)-[r:INTERSECTION]->(m) WHERE n.BBL in {[bbl for bbl in bbl_list]}"
        years_string = ""
        for year in year_list:
            years_string += "n:Lot" + str(year) + " OR "
        years_string = years_string[:-3]
        query_string += f" AND ({years_string})" 
        query_string += " RETURN n,r,m"
        print(query_string)
        return self.neo4j_conn.query(query_string, db='neo4j')
