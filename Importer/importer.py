import os
import time
import shapefile
from shapely.geometry import mapping, shape
import shutil
import matplotlib.pyplot as plt

def line_formatter(line):
    resp = []
    for v in line:
        if isinstance(v, str):
            v = v.replace(',',';')
        resp.append(str(v))
    return ','.join(resp).replace(',,',',NO DATA,').replace(',,',',NO DATA,')    
    
def relationship_maker_by_block_range_n_m_record_oid(n,m, initial_block_number, final_block_number, borough_name):
    tic_master = time.perf_counter()
    boroughs = {"BX": "Bronx", "BK": "Brooklyn", "MN": "Manhattan", "QN": "Queens", "SI": "Staten_Island"}
    try:
        os.mkdir("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number))
    except FileNotFoundError as e:
        os.mkdir("{}{}".format(n,m))
        os.mkdir("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number))
    except FileExistsError as e:
        print("Directory already exists, deleting")
        shutil.rmtree("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number), ignore_errors=True)
        os.mkdir("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number))
    
    with (open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,n), 'a') as nodes_n_csv, 
            open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m), 'a') as nodes_m_csv,
            open("{}{}/{}_{}_{}/edges.csv".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
            open("{}{}/{}_{}_{}/neo4j_commands.txt".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as neo4j):
        
        edges_csv.write("Source,Target,Area,AreaA,AreaB\n")
    
    nodes_records_n = []
    nodes_records_m = []
    nodes_shapes_n = []
    nodes_shapes_m = []
    index = 0
    fields_n_types = {}
    fields_m_types = {}

    shp1 = "MapPLUTO_{}v2/{}/{}MapPLUTO.shp".format(n,boroughs.get(borough_name),borough_name)
    shp2 = "MapPLUTO_{}v2/{}/{}MapPLUTO.shp".format(m,boroughs.get(borough_name),borough_name)    
    if (int(n) >= 18):
        shp1 = "MapPLUTO_{}v2/MapPLUTO.shp".format(n,borough_name)
    if (int(m) >= 18):
        shp2 = "MapPLUTO_{}v2/MapPLUTO.shp".format(m,borough_name)
    with (shapefile.Reader(shp1) as shp_n, 
        shapefile.Reader(shp2) as shp_m, 
        open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,n), 'a') as nodes_n_csv, 
        open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m), 'a') as nodes_m_csv,
        open("{}{}/{}_{}_{}/edges.csv".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
        open("{}{}/{}_{}_{}/neo4j_commands.txt".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as neo4j):
        
        if(os.stat("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,n)).st_size == 0):
            fields_n = ['YearBBL:row.YearBBL','Year:row.Year']
            for field in [row[0] for row in shp_n.fields][1:]:
                fields_n.append("{}:row.{}".format(field,field))
            
            fields_m = ['YearBBL:row.YearBBL','Year:row.Year']
            for field in [row[0] for row in shp_m.fields][1:]:
                fields_m.append("{}:row.{}".format(field,field))
            
            nodes_n_csv.write("YearBBL," + ','.join([row[0] for row in shp_n.fields][1:]) + ",Year\n")
            neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{n}.csv\" AS uri\n")
            neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
            neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(n,','.join(fields_n)))
            neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{m}.csv\" AS uri\n")
            neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
            neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(m,','.join(fields_m)))
            neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/edges.csv\" AS uri\n")
            neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
            neo4j.write("MATCH (source:Lot20{} {{YearBBL: row.Source}})\n".format(n))
            neo4j.write("MATCH (target:Lot20{} {{YearBBL: row.Target}})\n".format(m))
            neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")

            for record_row in shp_n.records():
                if record_row['Borough'] == borough_name and record_row['Block'] in range(initial_block_number,final_block_number+1):
                    nodes_records_n.append(record_row)
                    nodes_shapes_n.append(shp_n.shape(record_row.oid))
                    index += 1    
            index = 0
            for record_row in shp_m.records():
                if record_row['Borough'] == borough_name and record_row['Block'] in range(initial_block_number,final_block_number+1):
                    nodes_records_m.append(record_row)
                    nodes_shapes_m.append(shp_m.shape(record_row.oid))
                    index +=1

        if(os.stat("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m)).st_size == 0):
            nodes_m_csv.write("YearBBL," + ','.join([row[0] for row in shp_m.fields][1:]) + ",Year\n")

        for block_number in range(initial_block_number,final_block_number+1):
            tic = time.perf_counter()
            
            index = 0
            nodes_names_list = []        
            for record_row in nodes_records_n:
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    forma_shapely = shape(nodes_shapes_n[index].__geo_interface__)
                    if "20{}{}".format(n,record_row['BBL']) not in nodes_names_list:
                        nodes_names_list.append("20{}{}".format(n,record_row['BBL']))
                        nodes_n_csv.write("20{}{},{},20{}\n".format(n,record_row['BBL'],line_formatter(record_row[0:]),n))
                        for idx, item in enumerate(record_row[0:]):
                            if idx not in fields_n_types:
                                if isinstance(item, int):
                                    fields_n_types[idx] = 'int'
                                if isinstance(item, float):
                                    fields_n_types[idx] = 'float'
                                if isinstance(item, str):
                                    fields_n_types[idx] = 'str'                                        
                    index_aux = 0
                    for record_row_aux in nodes_records_m:
                        if record_row_aux['Block'] == block_number and record_row_aux['Borough'] == borough_name:
                            if "20{}{}".format(m,record_row_aux['BBL']) not in nodes_names_list:
                                nodes_names_list.append("20{}{}".format(m,record_row_aux['BBL']))
                                line_formatter(record_row_aux[0:])
                                nodes_m_csv.write("20{}{},{},20{}\n".format(m,record_row_aux['BBL'],line_formatter(record_row_aux[0:]),m))
                                for idx, item in enumerate(record_row_aux[0:]):
                                    if idx not in fields_m_types:
                                        if isinstance(item, int):
                                            fields_m_types[idx] = 'int'
                                        if isinstance(item, float):
                                            fields_m_types[idx] = 'float'
                                        if isinstance(item, str):
                                            fields_m_types[idx] = 'str'  
                            forma_shapely_aux = shape(nodes_shapes_m[index_aux].__geo_interface__)  

                            if(forma_shapely.intersects(forma_shapely_aux)):
                                intersect_area = forma_shapely.intersection(forma_shapely_aux).area
                                if intersect_area > 1:
                                    edges_csv.write("20{}{},20{}{},{},{},{}\n".format(n,record_row['BBL'],m,record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                    if(intersect_area/forma_shapely.area < 0.98 or intersect_area/forma_shapely_aux.area < 0.98):
                                        print("Found intersection ratio lower than 0.98, verify block {}".format(block_number))
                        index_aux += 1
                index += 1 
            print(f"Borough {borough_name}, Block {block_number}, took {time.perf_counter()- tic:0.4f} seconds")
        
        fields_n = ['YearBBL:toInteger(row.YearBBL)','Year:toInteger(row.Year)']
        for idx,field in enumerate([row[0] for row in shp_n.fields][1:]):
            if fields_n_types[idx] == 'int': 
                fields_n.append("{}:toInteger(row.{})".format(field,field))
            elif fields_n_types[idx] == 'float': 
                fields_n.append("{}:toFloat(row.{})".format(field,field))
            elif fields_n_types[idx] == 'str': 
                fields_n.append("{}:row.{}".format(field,field))
                
        fields_m = ['YearBBL:toInteger(row.YearBBL)','Year:toInteger(row.Year)']
        for idx,field in enumerate([row[0] for row in shp_m.fields][1:]):
            if fields_m_types[idx] == 'int': 
                fields_m.append("{}:toInteger(row.{})".format(field,field))
            elif fields_m_types[idx] == 'float': 
                fields_m.append("{}:toFloat(row.{})".format(field,field))
            elif fields_m_types[idx] == 'str': 
                fields_m.append("{}:row.{}".format(field,field))
        neo4j.write(f'\n\n')
        neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{n}.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(n,','.join(fields_n)))
        neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{m}.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(m,','.join(fields_m)))
        neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/edges.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("MATCH (source:Lot20{} {{YearBBL: toInteger(row.Source)}})\n".format(n))
        neo4j.write("MATCH (target:Lot20{} {{YearBBL: toInteger(row.Target)}})\n".format(m))
        neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")
    print(f"Borough {borough_name}, Block {initial_block_number} to Block {final_block_number}, took {time.perf_counter()- tic_master:0.4f} seconds")
    
#To use, call the function with the last two digits of the n year, the last two digits of the n+1 year, the initial block, the last block, and the borough as an abreviation
#e.g. to get all export the lots from the blocks 1 through 1000 of the years 2020 and 2021, from Manhatann you should call: 
#relationship_maker_by_block_range_n_m_record_oid(20, 21, 1, 1000, "MN")
        
