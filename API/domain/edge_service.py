from typing import List
from collections import Counter

from infrastructure.edge_repository import EdgeRepository
from infrastructure.data.filter import Filter

class EdgeService:

    def __init__(self) -> None:
        self.edge_repository = EdgeRepository()
        self.intersect_attribute = ['area','areaA','areaB']
        pass

    def get_edges_by_blocklist(self, year: int, initial_block: int, end_block: int, borough: str, filter_list: List[Filter]):
    
        edges = []
        left_edges = []
        right_edges = []
        for record in self.edge_repository.get_edges_by_blocklist(year, initial_block, end_block, borough, filter_list):
            left_lot = {"id" : record['r'].nodes[0].id}
            left_edges.append(record['r'].nodes[0].id)

            for item in record['r'].nodes[0].items():
                left_lot[item[0]] = item[1]
            
            intersection = {"id" : record['r'].id}
            for item in record['r'].items():
                intersection[item[0]] = item[1]
            
            right_lot = {"id" : record['r'].nodes[1].id}
            right_edges.append(record['r'].nodes[1].id)
            for item in record['r'].nodes[1].items():
                right_lot[item[0]] = item[1]
            
            edge = {'left_lot':left_lot, 'intersection': intersection, 'right_lot':right_lot}
            edges.append(edge)

    
        dict_left_edges = dict(Counter(left_edges))
        dict_right_edges = dict(Counter(right_edges))

        for edge in edges:
            edge['left_lot']['exit_edges'] = dict_left_edges[edge['left_lot']['id']]
            edge['right_lot']['incoming_edges'] = dict_right_edges[edge['right_lot']['id']]
        return edges

    def get_splits(self, year: int, initial_block: int, end_block: int, borough: str, filter_list: List[Filter]):

        edges = []
        left_edges = []
        right_edges = []
        for record in self.edge_repository.get_edges_by_blocklist(year, initial_block, end_block, borough, filter_list):
            left_lot = {"id" : record['r'].nodes[0].id}
            left_edges.append(record['r'].nodes[0].id)

            for item in record['r'].nodes[0].items():
                left_lot[item[0]] = item[1]
            
            intersection = {"id" : record['r'].id}
            for item in record['r'].items():
                intersection[item[0]] = item[1]
            
            right_lot = {"id" : record['r'].nodes[1].id}
            right_edges.append(record['r'].nodes[1].id)
            for item in record['r'].nodes[1].items():
                right_lot[item[0]] = item[1]
            
            edge = {'left_lot':left_lot, 'intersection': intersection, 'right_lot':right_lot}
            edges.append(edge)

    
        dict_left_edges = dict(Counter(left_edges))
        dict_right_edges = dict(Counter(right_edges))

        response = []
        for edge in edges:
            if dict_left_edges[edge['left_lot']['id']] >= 2: 
                edge['left_lot']['exit_edges'] = dict_left_edges[edge['left_lot']['id']]
                edge['right_lot']['incoming_edges'] = dict_right_edges[edge['right_lot']['id']]
                response.append(edge)
        return response

    def get_merges(self, year: int, initial_block: int, end_block: int, borough: str, filter_list: List[Filter]):
       
        edges = []
        left_edges = []
        right_edges = []
        for record in self.edge_repository.get_edges_by_blocklist(year, initial_block, end_block, borough, filter_list):
            left_lot = {"id" : record['r'].nodes[0].id}
            left_edges.append(record['r'].nodes[0].id)

            for item in record['r'].nodes[0].items():
                left_lot[item[0]] = item[1]
            
            intersection = {"id" : record['r'].id}
            for item in record['r'].items():
                intersection[item[0]] = item[1]
            
            right_lot = {"id" : record['r'].nodes[1].id}
            right_edges.append(record['r'].nodes[1].id)
            for item in record['r'].nodes[1].items():
                right_lot[item[0]] = item[1]
            
            edge = {'left_lot':left_lot, 'intersection': intersection, 'right_lot':right_lot}
            edges.append(edge)


        dict_left_edges = dict(Counter(left_edges))
        dict_right_edges = dict(Counter(right_edges))

        response = []
        for edge in edges:
            if dict_right_edges[edge['right_lot']['id']] >= 2: #and dict_left_edges[edge['left_lot']['id']] == 1:
                edge['left_lot']['exit_edges'] = dict_left_edges[edge['left_lot']['id']]
                edge['right_lot']['incoming_edges'] = dict_right_edges[edge['right_lot']['id']]
                response.append(edge)
        return response 

    def get_rearranges(self, year: int, initial_block: int, end_block: int, borough: str,filter_list: List[Filter]):
         
        edges = []
        left_edges = []
        right_edges = []
        for record in self.edge_repository.get_edges_by_blocklist(year, initial_block, end_block, borough, []): #filter_list):
            left_lot = {"id" : record['r'].nodes[0].id}
            left_edges.append(record['r'].nodes[0].id)

            for item in record['r'].nodes[0].items():
                left_lot[item[0]] = item[1]
            
            intersection = {"id" : record['r'].id}
            for item in record['r'].items():
                intersection[item[0]] = item[1]
            
            right_lot = {"id" : record['r'].nodes[1].id}
            right_edges.append(record['r'].nodes[1].id)
            for item in record['r'].nodes[1].items():
                right_lot[item[0]] = item[1]
            
            edge = {'left_lot':left_lot, 'intersection': intersection, 'right_lot':right_lot}
            edges.append(edge)

    
        dict_left_edges = dict(Counter(left_edges))
        dict_right_edges = dict(Counter(right_edges))

        rearrange_edges = []
        rearrange_bbl = []
        for edge in edges:
            if dict_right_edges[edge['right_lot']['id']] >= 2 and dict_left_edges[edge['left_lot']['id']] >= 2:
                edge['left_lot']['exit_edges'] = dict_left_edges[edge['left_lot']['id']]
                edge['right_lot']['incoming_edges'] = dict_right_edges[edge['right_lot']['id']]
                if(self.filter_edge(edge,filter_list)):
                    rearrange_edges.append(edge)
                
                rearrange_bbl.append(edge['left_lot']['YearBBL'])
                rearrange_bbl.append(edge['right_lot']['YearBBL'])
        splits = self.get_splits(year, initial_block, end_block, borough, [])
        for edge in splits:
            if (edge['left_lot']['YearBBL'] in rearrange_bbl or edge['right_lot']['YearBBL'] in rearrange_bbl):
                rearrange_edges.append(edge)
                rearrange_bbl.append(edge['left_lot']['YearBBL'])
                rearrange_bbl.append(edge['right_lot']['YearBBL'])
        merges = self.get_merges(year, initial_block, end_block, borough, [])
        for edge in merges:
            if (edge['left_lot']['YearBBL'] in rearrange_bbl or edge['right_lot']['YearBBL'] in rearrange_bbl):
                rearrange_edges.append(edge)
                rearrange_bbl.append(edge['left_lot']['YearBBL'])
                rearrange_bbl.append(edge['right_lot']['YearBBL'])
        return rearrange_edges

    def insert_edge_ordered(self, edges_list, edge_to_insert):
        resp = []
        if len(edges_list) == 0: return [edge_to_insert]
        inserted = False
        for edge in edges_list:
            if(edge_to_insert['left_lot']['Block'] < edge['left_lot']['Block']):
                resp.append(edge_to_insert)
                inserted = True
            resp.append(edge)
        if not inserted:
            resp.append(edge_to_insert)
        return resp

    def filter_edge(self, edge, filter_list):
        for filter in filter_list:
            if filter.attribute not in self.intersect_attribute:
                result = {
                    '>': lambda attribute, value: edge['left_lot'][attribute] > value or edge['right_lot'][attribute] > value,
                    '<': lambda attribute, value: edge['left_lot'][attribute] < value or edge['right_lot'][attribute] < value,
                    '>=': lambda attribute, value: edge['left_lot'][attribute] >= value or edge['right_lot'][attribute] >= value,
                    '<=': lambda attribute, value: edge['left_lot'][attribute] <= value or edge['right_lot'][attribute] <= value,
                    '=': lambda attribute, value: edge['left_lot'][attribute] == value or edge['right_lot'][attribute] == value,
                    '<>': lambda attribute, value: edge['left_lot'][attribute] != value or edge['right_lot'][attribute] != value,
                    }[filter.operand](filter.attribute, filter.value)
                if not result: return False
            else:
                result = {
                    '>': lambda attribute, value: edge['intersection'][attribute] > value,
                    '<': lambda attribute, value: edge['intersection'][attribute] < value,
                    '>=': lambda attribute, value: edge['intersection'][attribute] >= value,
                    '<=': lambda attribute, value: edge['intersection'][attribute] <= value,
                    '=': lambda attribute, value: edge['intersection'][attribute] == value,
                    '<>': lambda attribute, value: edge['intersection'][attribute] != value,
                    }[filter.operand](filter.attribute, filter.value)
                if not result: return False    
        return True

    def get_edges_by_bbl(self, bbl_list: List[int], initial_year: int, end_year: int):
        edges = []
        left_edges = []
        right_edges = []
        for record in self.edge_repository.get_edges_by_bbl(bbl_list, initial_year, end_year):
            left_lot = {"id" : record['r'].nodes[0].id}
            left_edges.append(record['r'].nodes[0].id)

            for item in record['r'].nodes[0].items():
                left_lot[item[0]] = item[1]
            
            intersection = {"id" : record['r'].id}
            for item in record['r'].items():
                intersection[item[0]] = item[1]
            
            right_lot = {"id" : record['r'].nodes[1].id}
            right_edges.append(record['r'].nodes[1].id)
            for item in record['r'].nodes[1].items():
                right_lot[item[0]] = item[1]
            
            edge = {'left_lot':left_lot, 'intersection': intersection, 'right_lot':right_lot}
            edges.append(edge)

    
        dict_left_edges = dict(Counter(left_edges))
        dict_right_edges = dict(Counter(right_edges))

        for edge in edges:
            edge['left_lot']['exit_edges'] = dict_left_edges[edge['left_lot']['id']]
            edge['right_lot']['incoming_edges'] = dict_right_edges[edge['right_lot']['id']]
        return edges     
      
              
    def get_rearranges_ids(self, year: int, initial_block: int, end_block: int, borough: str, filter_list: List[Filter]): 
        edges = []
        left_edges = []
        right_edges = []
        for record in self.edge_repository.get_edges_by_blocklist(year, initial_block, end_block, borough, []): #filter_list):
            left_lot = {"id" : record['r'].nodes[0].id}
            left_edges.append(record['r'].nodes[0].id)

            for item in record['r'].nodes[0].items():
                left_lot[item[0]] = item[1]
            
            intersection = {"id" : record['r'].id}
            for item in record['r'].items():
                intersection[item[0]] = item[1]
            
            right_lot = {"id" : record['r'].nodes[1].id}
            right_edges.append(record['r'].nodes[1].id)
            for item in record['r'].nodes[1].items():
                right_lot[item[0]] = item[1]
            
            edge = {'left_lot':left_lot, 'intersection': intersection, 'right_lot':right_lot}
            edges.append(edge)

        dict_left_edges = dict(Counter(left_edges))
        dict_right_edges = dict(Counter(right_edges))

        rearrange_edges = []
        rearrange_bbl = []
        for edge in edges:
            if dict_right_edges[edge['right_lot']['id']] >= 2 and dict_left_edges[edge['left_lot']['id']] >= 2:
                edge['left_lot']['exit_edges'] = dict_left_edges[edge['left_lot']['id']]
                edge['right_lot']['incoming_edges'] = dict_right_edges[edge['right_lot']['id']]
                if(self.filter_edge(edge,filter_list)):
                    rearrange_edges.append(edge)
                rearrange_bbl.append(edge['left_lot']['YearBBL'])
                rearrange_bbl.append(edge['right_lot']['YearBBL'])
        
        splits = self.get_splits(year, initial_block, end_block, borough, [])
        for edge in splits:
            if (edge['left_lot']['YearBBL'] in rearrange_bbl or edge['right_lot']['YearBBL'] in rearrange_bbl):
                rearrange_edges.append(edge)
                rearrange_bbl.append(edge['left_lot']['YearBBL'])
                rearrange_bbl.append(edge['right_lot']['YearBBL'])

        merges = self.get_merges(year, initial_block, end_block, borough, [])
        for edge in merges:
            if (edge['left_lot']['YearBBL'] in rearrange_bbl or edge['right_lot']['YearBBL'] in rearrange_bbl):
                rearrange_edges.append(edge)
                rearrange_bbl.append(edge['left_lot']['YearBBL'])
                rearrange_bbl.append(edge['right_lot']['YearBBL'])
        
        rearrange_ids = {}

        for item in rearrange_edges:
            if item['left_lot']['YearBBL'] not in rearrange_ids:
                rearrange_ids[item['left_lot']['YearBBL']] = item['intersection']['id']
            if item['right_lot']['YearBBL'] not in rearrange_ids:
                rearrange_ids[item['right_lot']['YearBBL']] = item['intersection']['id']
            for aux in rearrange_edges:
                if aux['left_lot']['YearBBL'] in rearrange_ids:
                    if aux['right_lot']['YearBBL'] in rearrange_ids:
                        rearrange_ids[aux['left_lot']['YearBBL']] = rearrange_ids[aux['right_lot']['YearBBL']]
                    else:
                        rearrange_ids[aux['right_lot']['YearBBL']] = rearrange_ids[aux['left_lot']['YearBBL']]
                if aux['right_lot']['YearBBL'] in rearrange_ids:
                    if aux['left_lot']['YearBBL'] in rearrange_ids:
                        rearrange_ids[aux['right_lot']['YearBBL']] = rearrange_ids[aux['left_lot']['YearBBL']]
                    else:
                        rearrange_ids[aux['left_lot']['YearBBL']] = rearrange_ids[aux['right_lot']['YearBBL']]

        return rearrange_ids
    
    