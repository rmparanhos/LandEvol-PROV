# LandEvol-PROV
The LandEvol-PROV is an approach to analysis and management of urban geospatial data, more specificaly, from the MapPLUTO dataset of New York City, presented on the papers:

* [Uso de Grafos de Proveniência para Análise Temporal de Uso do Solo em Centros Urbanos: uma Abordagem Prática](https://sol.sbc.org.br/index.php/sbbd/article/view/25560), SBBD 2023
* FCGS

All the code was developed with `Python 3` and the dependencies can be resolved with each project `requirements.txt`. For more information, please refer to the apointed papers.

## API
The API was developed using `FastAPI`, and can be executed with the command:

```
uvicorn main:app --reload
```

After this, the swagger documentation can be accessed from the route `/docs`. The four available functions are:

* `/edges`
* `/splits`
* `/merges`
* `/rearranges`

For the API to work, it needs a Neo4J instance running. If necessary the address of the database can be changed on the file `infrastructure
/edge_repository.py`. There`s also a Neo4J backup available on this repository, for this, check the **Graph Database** section.

## Importer
To use, call the function with the last two digits of the n year, the last two digits of the n+1 year, the initial block, the last block, and the borough as an abreviation. e.g. to get all export the lots from the blocks 1 through 1000 of the years 2018 and 2019, from Manhatann you should call:

```
relationship_maker_by_block_range_n_m_record_oid(18, 19, 1, 1000, "MN")
``` 
The result files to be executed on the Neo4J will be on a directory with the name as the two years joined, for the before execution, the result will be on a directory called `1819`.


The MapPLUTO files shoud be put in a directory, without changing the folder structure after downloading the files from NYC Department of City Planning website, with the following name convention:

```
MapPLUTO_XYv2
``` 

Where XY are the last digits of the year of the file. e.g. for the 2021 file it should be `MapPLUTO_21v2`. 

## Graph Database

The graph database is the result of the conversion using the Importer, for this paper we have all the lots from 2009 to 2021 from the boroughs of Manhatann and Brooklyn. This database backup from Neo4J was obtained using the `neo4j-admin dump` tool, and can be imported to your own Neo4J instance with the command `neo4j-admin database load`.

## Files
There are three files used for an analysis of the block 706 of Manhatann:
* `706-neo4j.json` is the block as obtained from the Neo4J
* `706.json` is the block converted to the PROV-JSON format
* `706-rearrange.json` is the block converted to the PROV-JSON format with the rearranges obtained

