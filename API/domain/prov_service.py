from typing import List
from collections import Counter


class ProvService:

    def __init__(self) -> None:
        pass

    def convert_to_prov_json(self, edges: List[any], merges: List[any], splits: List[any], rearranges: List[any], with_attributes=True, with_rearranges=True):
        prov = {}
        prefix = {}
        prefix['xsd'] = "http://www.w3.org/2001/XMLSchema#"
        prefix['prov'] = "http://www.w3.org/ns/prov#"
        prefix['provviz'] = "http://provviz/ns/provviz#"
        prov['prefix'] = prefix
        prov['entity'] = {}
        prov['wasDerivedFrom'] = {}
        prov['activity'] = {}
        prov['wasGeneratedBy'] = {}
        prov['used'] = {}
        
        merges_intersect_list = []
        merges_right_lot_list = []
        for merge in merges:
            if with_attributes:
                if "prov:Merge" + str(merge['right_lot']['id']) not in prov['activity']:
                    prov['activity']["prov:Merge" + str(merge['right_lot']['id'])] = {"prov:Year": merge['left_lot']['Year']}
                #prov['activity']["prov:Merge" + str(merge['right_lot']['id'])]["prov:areaA_" + str(merge['right_lot']['YearBBL'])] = merge['intersection']['areaA']
                prov['activity']["prov:Merge" + str(merge['right_lot']['id'])]["prov:area"] = merge['intersection']['area']
                prov['activity']["prov:Merge" + str(merge['right_lot']['id'])]["prov:areaB_" + str(merge['left_lot']['YearBBL'])] = merge['intersection']['areaB']
            else:
                prov['activity']["prov:Merge" + str(merge['right_lot']['id'])] = {}
            merges_right_lot_list.append(merge['right_lot']['id'])
            merges_intersect_list.append(merge['intersection']['id'])
        
        splits_intersect_list = []
        splits_left_lot_list = []
        for split in splits:
            if with_attributes:
                if "prov:Split" + str(split['left_lot']['id']) not in prov['activity']:
                    prov['activity']["prov:Split" + str(split['left_lot']['id'])] = {"prov:Year": split['left_lot']['Year']}
                #prov['activity']["prov:Split" + str(split['left_lot']['id'])]["prov:areaB_" + str(split['left_lot']['YearBBL'])] = split['intersection']['areaB']
                prov['activity']["prov:Split" + str(split['left_lot']['id'])]["prov:area"] = split['intersection']['area']
                prov['activity']["prov:Split" + str(split['left_lot']['id'])]["prov:areaA_" + str(split['right_lot']['YearBBL'])] = split['intersection']['areaA']
            else:
                prov['activity']["prov:Split" + str(split['left_lot']['id'])] = {}
            splits_left_lot_list.append(split['left_lot']['id'])
            splits_intersect_list.append(split['intersection']['id'])
            
        for edge in edges:
            if with_attributes:
                prov['entity']["prov:" + str(edge['left_lot']['YearBBL'])] = {"prov:LotArea": edge['left_lot']['LotArea']}
                prov['entity']["prov:" + str(edge['right_lot']['YearBBL'])] = {"prov:LotArea": edge['right_lot']['LotArea']}
            else:
                prov['entity']["prov:" + str(edge['left_lot']['YearBBL'])] = {}
                prov['entity']["prov:" + str(edge['right_lot']['YearBBL'])] = {}
            
            ##Merges
            if edge['right_lot']['id'] in merges_right_lot_list:   
                wGB = f"_:wGBMerge_{edge['right_lot']['id']}"
                wGBdict = {}
                wGBdict["prov:entity"] = "prov:" + str(edge['right_lot']['YearBBL'])
                wGBdict["prov:activity"] = "prov:Merge" + str(edge['right_lot']['id'])
                prov['wasGeneratedBy'][wGB] = wGBdict
            
            if edge['intersection']['id'] in merges_intersect_list:                   
                u = f"_:uMerge_{edge['left_lot']['id']}_{edge['intersection']['id']}"
                udict = {}
                udict["prov:activity"] = "prov:Merge" + str(edge['right_lot']['id'])
                udict["prov:entity"] = "prov:" + str(edge['left_lot']['YearBBL'])
                prov['used'][u] = udict
           
            ##splits
            if edge['left_lot']['id'] in splits_left_lot_list:    
                wGB = f"_:wGBSplit_{edge['right_lot']['id']}_{edge['intersection']['id']}"
                wGBdict = {}
                wGBdict["prov:entity"] = "prov:" + str(edge['right_lot']['YearBBL'])
                wGBdict["prov:activity"] = "prov:Split" + str(edge['left_lot']['id'])
                prov['wasGeneratedBy'][wGB] = wGBdict
                
                
            if edge['intersection']['id'] in splits_intersect_list:   
                u = f"_:uSplit_{edge['left_lot']['id']}"
                udict = {}
                udict["prov:activity"] = "prov:Split" + str(edge['left_lot']['id'])
                udict["prov:entity"] = "prov:" + str(edge['left_lot']['YearBBL'])
                prov['used'][u] = udict
                             
            
            ##rearranges
            if(with_rearranges):
                if edge['left_lot']['YearBBL'] in rearranges[str(edge['left_lot']['Year']) + str(edge['left_lot']['Year']+1)]:
                    u = f"_:uRearrange_{edge['left_lot']['id']}"
                    udict = {}
                    udict["prov:activity"] = "prov:Rearrange" + str(rearranges[str(edge['left_lot']['Year']) + str(edge['left_lot']['Year']+1)][edge['left_lot']['YearBBL']])
                    udict["prov:entity"] = "prov:" + str(edge['left_lot']['YearBBL'])
                    prov['used'][u] = udict
                    
                    if f"_:uMerge_{edge['left_lot']['id']}_{edge['intersection']['id']}" in prov['used']:
                        prov['activity'].pop(prov['used'][f"_:uMerge_{edge['left_lot']['id']}_{edge['intersection']['id']}"]['prov:activity'], None)
                    if f"_:uSplit_{edge['left_lot']['id']}" in prov['used']:
                        prov['activity'].pop(prov['used'][f"_:uSplit_{edge['left_lot']['id']}"]['prov:activity'], None)
                    prov['used'].pop(f"_:uMerge_{edge['left_lot']['id']}_{edge['intersection']['id']}", None)
                    prov['used'].pop(f"_:uSplit_{edge['left_lot']['id']}", None)

                if edge['right_lot']['YearBBL'] in rearranges[str(edge['right_lot']['Year']-1) + str(edge['right_lot']['Year'])]:
                    wGB = f"_:wGBRearrange_{edge['right_lot']['id']}"
                    wGBdict = {}
                    wGBdict["prov:entity"] = "prov:" + str(edge['right_lot']['YearBBL'])
                    wGBdict["prov:activity"] = "prov:Rearrange"  + str(rearranges[str(edge['right_lot']['Year']-1) + str(edge['right_lot']['Year'])][edge['right_lot']['YearBBL']])
                    prov['wasGeneratedBy'][wGB] = wGBdict
                    
                    if f"_:wGBMerge_{edge['right_lot']['id']}" in prov['wasGeneratedBy']:
                        prov['activity'].pop(prov['wasGeneratedBy'][f"_:wGBMerge_{edge['right_lot']['id']}"]['prov:activity'], None)
                    if f"_:wGBSplit_{edge['right_lot']['id']}_{edge['intersection']['id']}" in prov['wasGeneratedBy']:
                        prov['activity'].pop(prov['wasGeneratedBy'][f"_:wGBSplit_{edge['right_lot']['id']}_{edge['intersection']['id']}"]['prov:activity'], None)
                    prov['wasGeneratedBy'].pop(f"_:wGBMerge_{edge['right_lot']['id']}", None)
                    prov['wasGeneratedBy'].pop(f"_:wGBSplit_{edge['right_lot']['id']}_{edge['intersection']['id']}", None)
        
            
            ##maintain    
            if edge['intersection']['id'] not in splits_intersect_list and edge['intersection']['id'] not in merges_intersect_list:
                if with_attributes:
                    prov['activity']["prov:Maintain" + str(edge['left_lot']['id'])] = {"prov:Year": edge['left_lot']['Year']}
                    prov['activity']["prov:Maintain" + str(edge['left_lot']['id'])]["prov:Area_Old" + str(edge['left_lot']['YearBBL'])] = edge['left_lot']['LotArea']
                    prov['activity']["prov:Maintain" + str(edge['left_lot']['id'])]["prov:Area_New" + str(edge['right_lot']['YearBBL'])] = edge['right_lot']['LotArea']
                    prov['activity']["prov:Maintain" + str(edge['left_lot']['id'])]["prov:Area_Intersect" + str(edge['intersection']['id'])] = edge['intersection']['area']
                else:
                    prov['activity']["prov:Maintain" + str(edge['left_lot']['id'])] = {}
                wGB = f"_:wGBMaintain_{edge['left_lot']['id']}"
                wGBdict = {}
                wGBdict["prov:entity"] = "prov:" + str(edge['right_lot']['YearBBL'])
                wGBdict["prov:activity"] = "prov:Maintain" + str(edge['left_lot']['id'])
                prov['wasGeneratedBy'][wGB] = wGBdict
                u = f"_:uMaintain_{edge['left_lot']['id']}"
                udict = {}
                udict["prov:activity"] = "prov:Maintain" + str(edge['left_lot']['id'])
                udict["prov:entity"] = "prov:" + str(edge['left_lot']['YearBBL'])
                prov['used'][u] = udict       
        
        if(with_rearranges):
            for year in rearranges:
                for rearrange in rearranges[year]:
                    if with_attributes:
                        prov['activity']["prov:Rearrange" + str(rearranges[year][rearrange])] = {"prov:Year": int(year[:-4])}
                    else:    
                        prov['activity']["prov:Rearrange" + str(rearranges[year][rearrange])] = {}
                
        return prov
