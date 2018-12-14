import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from functools import reduce
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import rasterio
from rasterio.transform import from_origin
import io
from subprocess import call

## Variables de la base de datos

db = 'cadenas'
usr = 'postgres' 
psw = 'postgres' 
host = '192.168.18.22'
con = psycopg2.connect(database= db, user=usr, password=psw, host=host)
engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(usr,psw,host,db))

## Lista con los nombres de las tablas de eatapas, esta debería obtenerse del usuario
etapas = ['etapa1', 'etapa2', 'etapa3', 'etapa4' ]


## Funciones Auxiliares
def node_relations(table, engine, connection):
    """Toma una tabla de puntos y le agrega una columna con el id del
       nodo más cercano de la red.
       Elimina la tabla de relaciones en caso de que exista.
       Regresa el dataframe con la relación.
    """
    drop_qry = """drop table if exists %(tabla)s_node""" % {"tabla": table}
    curs = connection.cursor()
    curs.execute(drop_qry)
    connection.commit()
    sql = """
            select f.id as id_%(tabla)s, (
              SELECT n.id
              FROM red_vertices_pgr As n
              ORDER BY f.geom <-> n.geom LIMIT 1
            )as closest_node
            from %(tabla)s f
          """ % {"tabla": table}
    try:
        df = pd.read_sql(sql, engine)
    except ValueError as e:
        print(e)
    try:
        df.to_sql(table + '_node', engine)
    except ValueError as e:
        print(e)
    return df

def stage_cost(source_table, target_table, cost_column):
    params = {'source': source_table, 'target': target_table, 'cost': cost_column }
    qry_str = """SELECT DISTINCT ON (start_vid)
                 start_vid as id_%(source)s, end_vid as id_%(target)s, agg_cost as costo_%(source)s_%(target)s
          FROM   (SELECT * FROM pgr_dijkstraCost(
              'select id, source, target, %(cost)s as cost from red',
              array(select distinct(s.closest_node) from (select e.*, r.closest_node
                                                        from %(source)s e
                                                        join %(source)s_node r
                                                        on e.id = r.id_%(source)s::int) as s),
              array(select distinct(t.closest_node) from (select e.*, r.closest_node
                                                        from %(target)s e
                                                        join %(target)s_node r
                                                        on e.id = r.id_%(target)s::int) as t),
                 directed:=false)
          ) as sub
          ORDER  BY start_vid, agg_cost asc""" % params
    try:
        df = pd.read_sql(qry_str, engine)
    except ValueError as e:
        print(e)
    return df


## Creamos las relaciones entre etapas y nodos de la red
## NOTA: Como la relación (tabla en la base de datos) se elimina cada vez que se llama a la función 
## puede haber dificultades cuando dos o más usuarios usan la aplicación, valdría la pena agregar un id de ususrio a los
## nombres de las tablas.
node_relations_list = []
for etapa in etapas:
    node_relations_list.append(node_relations(etapa, engine, con))
    
## Leemos la geometría de cada etapa con su identificador de closes node (lo vamos a usar más adelante)
etapas_gdfs = []
for etapa in etapas:
    sql = """select a.id, a.geom, b.closest_node
             from %(etapa)s a
             join %(etapa)s_node b
             on a.id = b.id_%(etapa)s""" % {"etapa":etapa}
    etapas_gdfs.append(gpd.GeoDataFrame.from_postgis(sql, con, geom_col='geom'))
    
## Calculamos las distanicas entre etapas
distancias = []
for i, etapa in enumerate(etapas): 
    if i < len(etapas)-1:
        stage = stage_cost(etapa,etapas[i+1], "costo")
        cost_col = list(stage.columns)[-1]
        stage.columns = ['start_' + str(i), 'end_' + str(i), cost_col]
        stage.to_sql('dist_' + etapa, engine, index=False, if_exists='replace')
        distancias.append(stage)

## Hacemos un dataframe con las distancias entre etapas
for i, distancia in enumerate(distancias):
    print(i)
    if i == 0:
        costos = pd.merge(distancia, distancias[1], left_on='end_0', right_on='start_1')
    elif i < len(distancias) - 1:
        costos = pd.merge(costos, distancias[i+1], left_on='end_' + str(i), right_on='start_' + str(i+1))
        
## Calculamos los costos acumulados, para los rasters, y conservamos el id de la primera etapa (porque es la geometría que nos interesa)
costos_acumulados = costos.iloc[:, costos.columns.str.contains('costo_')].cumsum(axis=1)
costos_acumulados = costos_acumulados.merge(costos.iloc[:,[0]], left_index=True, right_index=True)

## Agregamos la geometría a los costos acumulados
costos_acumulados = etapas_gdfs[0].merge(costos_acumulados, left_on = 'closest_node', right_on = 'start_0')

## Escribimos los rasters en la carpeteta idw
for columna in costos_acumulados:
    if columna.startswith('costo_'):
        write_me = costos_acumulados[['id', 'geom', columna]]
        write_me.columns = ['id', 'geom', 'costo']
        write_me.to_file(driver= 'ESRI Shapefile', filename= "idw/" + columna + '.shp')
        comando = ['gdal_grid', '-zfield', 'costo', '-l', columna, '-a',
           'invdist:power=2.0:smothing=0.0:radius1=0.0:radius2=0.0:angle=0.0:max_points=0:min_points=0:nodata=0.0',
           '-of', 'GTiff', 'idw/'+ columna + '.shp', 'idw/'+ columna+'.tif']
    try:
        call(comando)
    except:
        print('valió verga')
    print('La columna '+columna+' no valió verga')

    
## Escribimos en la base de datos los polígonos (areas de servicio) para cada etapa
for j,(etapa, etapa_gdf) in enumerate(zip(etapas[0:-1],etapas_gdfs[1:])):
    drop_qry = """drop table if exists poligono_%(etapa)s""" % {'etapa':etapa}
    curs = con.cursor()
    curs.execute(drop_qry)
    con.commit()        
    create_sql = """create table poligono_%(etapa)s (id_%(etapa)s bigint, \
                    geom geometry(Polygon,32615))""" % {'etapa':etapa}
    curs.execute(create_sql)
    con.commit()    
    ids = etapa_gdf['closest_node'].unique()
    for i, id in enumerate(ids):
        point_sql = """select e.id::int4, st_x (st_geometryn(e.geom,1)) as x, st_y  (st_geometryn(e.geom,1)) as y 
            from 
            (select c.geom, d.end_%(num_etapa)s as id
            from 
            (select a.geom, b.closest_node 
            from %(etapa)s a
            join %(etapa)s_node b
            on a.id = b.id_%(etapa)s) as c
            join dist_%(etapa)s d
            on c.closest_node = d.start_%(num_etapa)s) as e 
            where id=%(de_quien)s
            """ % {'etapa':etapa, 'num_etapa':j, 'de_quien':id}
        point_gdf = pd.read_sql(point_sql, con)
        if point_gdf.shape[0] > 2:
            poly_sql = """
                insert into poligono_%(etapa)s
                select sub.id, sub.geom
                from
                (select %(de_quien)s as id, * from st_setsrid(pgr_pointsAsPolygon('select e.id::int4, st_x (st_geometryn(e.geom,1)) as x, st_y  (st_geometryn(e.geom,1)) as y 
                from 
                (select c.geom, d.end_%(num_etapa)s as id
                from 
                (select a.geom, b.closest_node 
                from %(etapa)s a
                join %(etapa)s_node b
                on a.id = b.id_%(etapa)s) as c
                join dist_%(etapa)s d
                on c.closest_node = d.start_%(num_etapa)s) as e 
                where id=%(de_quien)s'),32615)as geom)as sub 
            """ % {'etapa':etapa, 'num_etapa':j, 'de_quien':id}
            curs.execute(poly_sql)
            con.commit()

## Cerramos la conexión
con.close()
engine.dispose()