import geopandas as gpd
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

def update_geojson(filename: str, parameters: dict):
    import json
    
    def _get_value(data, parameter):
        return data['features'][0]['properties'][parameter]
    
    def _set_value(data, parameter, value):
        data['features'][0]['properties'][parameter] = value
    
    #Read json file
    def _read_json(filename: str):
        print(f'Read {filename}')
        with open(filename, 'r') as file:
            data = json.load(file)
            job_name = _get_value(data, 'aoi_name') + '_' + \
                       str(_get_value(data, 'path_number'))
            _set_value(data, 'job_name', job_name)
            _set_value(data, 'pathNumber', _get_value(data, 'path_number'))

        return data
    
    #Write down json file
    def _write_json(filename: str, data: dict):
        print(f'Write {filename}')
        with open(filename, 'w') as file:
            json.dump(data, file)
                        
    # Print parameters from geojson      
    def _get_json_data(data):
        parameter_list = ['aoi_name', 'path_number','azimuth_mismatch',
                          'num_neighbors','month_range_lower', 'month_range_upper',
                          'min_days_backward_timesubset', 'temporal_window_days_timesubset', 
                          'num_neighbors_timesubset', 'job_name']

        print('Geojson parameters:')
        for parameter in parameter_list:
            value = _get_value(data, parameter)
            print(f'  {parameter} : {value}')
    
    # Update the parameters in json file      
    def _update(data: dict, 
                parameter: str, 
                parameter_data: int, 
                update: bool = False):
        json_keys = data['features'][0]['properties'].keys()
        if parameter not in json_keys or update:
            print(f'Updating json: {parameter} = {parameter_data}')
            if parameter in ['min_days_backward_timesubset','num_neighbors_timesubset']:
                if parameter_data != []:
                    parameter_data = ','.join(map(str, parameter_data))

                if parameter == 'min_days_backward_timesubset':
                    twindow = int(_get_value(data,'temporal_window_days_timesubset'))
                    parameter_data = [int(i) - round(twindow/2) for i in parameter_data.split(',')]
                    parameter_data = ','.join(map(str, parameter_data))

            _set_value(data, parameter, parameter_data)

        return data
    
    # Sanity check
    def _check_json(data):
        if _get_value(data,'min_days_backward_timesubset'):
            min_days_backward_timesubset = [int(s) for s in 
                                            _get_value(data,'min_days_backward_timesubset').split(',')]

            if any(x<1 for x in min_days_backward_timesubset):
                raise Exception("Your specified 'min_days_backward_timesubset' input is too ",
                                "small relative to your specified 'temporal_window_days_timesubset'",
                                "value. Adjust accordingly")

            if _get_value(data,'num_neighbors_timesubset'):
                num_neighbors_timesubset = [int(s) for s in 
                                            _get_value(data, 'num_neighbors_timesubset').split(',')]

                if len (num_neighbors_timesubset) != len(min_days_backward_timesubset):
                    raise Exception("Specified number of temporal sampling intervals \
                                    DO NOT match specified nearest neighbor sampling")

    ## Main ####
    data = _read_json(filename)
    
    for p in parameters:
        _update(data, p, parameters[p], parameters['update_AO'])
    
    _get_json_data(data)
    
    _check_json(data)
    if parameters['update_AO']:
        _write_json(filename, data)

    return gpd.read_file(filename)

################################ OPERATIONS ON DF ####################################
from shapely.geometry import shape
from shapely.geometry import Polygon, GeometryCollection
from rasterio.crs import CRS
import numpy as np

def split_polygon(polygon: Polygon, split_lat: float) -> GeometryCollection:
    from shapely.ops import split
    from shapely.geometry import LineString
    
    [x1, y1, x2, y2] = polygon.bounds
    #create line for splitting polygon
    dy = np.mean([abs(y2- np.round(y2)), abs(y1- np.round(y1))])
    split_line = LineString([(x1 - 1, split_lat), (x2 + 1, split_lat + dy)])
    print('Split line:', split_line)
    return split(polygon, split_line)

def separate_df_geometries(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df =df.explode(index_parts=True).droplevel(0)
    n = df.shape[0]
    new_dfs = []
    for i in range(n):
        new_dfs.append(gpd.GeoDataFrame(df.iloc[i].to_frame().T, crs=CRS.from_epsg(4326)))
    return new_dfs

def split_aoi(df_aoi: gpd.GeoDataFrame, split_lats: list) -> gpd.GeoDataFrame:
    #Create dataframe for aoi_geometry
    path_dict = {}
    path_dict['pathNumber'] = str(df_aoi.path_number[0])
    aoi_geometry = pd.DataFrame([path_dict])
    aoi_geometry = gpd.GeoDataFrame(aoi_geometry, 
                                    geometry=[shape(df_aoi.geometry.unary_union)], 
                                    crs=CRS.from_epsg(4326))
    #Sort the list
    split_lats = sorted(split_lats)
    
    #get aoi
    rm_flag = False
    for i, lat in enumerate(split_lats):
        print(f'Split AOI at: {lat: .2f} lat')
        if i > 0 and len(aoi.geoms)>1:
            aoi_bounds = aoi.geoms[1]
            aoi = split_polygon(aoi_bounds, float(lat))
        else:
            if i != 0:
                rm_flag = True
            aoi_bounds = aoi_geometry.unary_union
            aoi = split_polygon(aoi_bounds, float(lat))
        aoi_geometry.loc[i] = (df_aoi.path_number[0], shape(aoi.geoms[0]))
        #Add the last part 
        if i == len(split_lats) - 1 and len(aoi.geoms)>1:
            aoi_geometry.loc[i+1] = (df_aoi.path_number[0], shape(aoi.geoms[1]))
    aoi_geometry.crs = CRS.from_epsg(4326)
    
    if rm_flag:
        aoi_geometry = aoi_geometry.iloc[1: , :]
      
    return aoi_geometry



################################# PLOTTING ###############################

def overlap_debug_plots(aoi: gpd.GeoDataFrame, overlap_dict:dict, ptype: str = 'good_coverage') -> None:
    type_options = ['good_coverage', 'rejected_coverage', 'rejected_scenes', 'gap_scenes']
    
    if ptype not in type_options:
        raise Exception(f'Selected option unavailable, plot type options are {type_options}')
    
    rejected_scenes_dict =  overlap_dict['rejected_scenes_dict'].sort_values(by=['start_date'])
    gap_scenes_dict =  overlap_dict['gap_scenes_dict'].sort_values(by=['start_date'])
    
    # Plot all mosaicked acquisitions that meet user-defined spatial coverage
    if ptype == 'good_coverage':
        fig, ax = plt.subplots()
        for index, row in overlap_dict['stack_dict'].iterrows():
            p = gpd.GeoSeries(row['geometry'])
            p.exterior.plot(color='green', facecolor='green', alpha=0.2, ax=ax)
        aoi.exterior.plot(color='black', ax=ax, label='AOI')
        plt.legend()

    # Plot all individual mosaicked acquisitions that were rejected 
    #     for not meeting user-specified spatial constraints
    elif ptype == 'rejected_coverage':
        fig, ax = plt.subplots()
        for index, row in rejected_scenes_dict.iterrows():
            p = gpd.GeoSeries(row['geometry'])
            p.exterior.plot(color='red',  facecolor='red', alpha=0.2, ax=ax)
        aoi.exterior.plot(color='black', ax=ax, label='AOI')
        plt.legend()
            
    # Plot all individual mosaicked acquisitions that were rejected 
    #     for not meeting user-specified spatial constraints
    elif ptype == 'rejected_scenes':
        for index, row in rejected_scenes_dict.iterrows():
            fig, ax = plt.subplots()
            p = gpd.GeoSeries(row['geometry'])
            p.exterior.plot(color='red', facecolor='red', alpha=0.2, ax=ax, label=row['start_date_str'][0])
            aoi.exterior.plot(color='black', ax=ax, label='AOI')
            plt.legend()

                
    # Plot acquisitions that aren't continuous (i.e. have gaps)
    elif ptype == 'gap_scenes':
        for index, row in gap_scenes_dict.iterrows():
            fig, ax = plt.subplots()
            p = gpd.GeoSeries(row['geometry'])
            p.exterior.plot(color='blue', facecolor='blue', alpha=0.2, ax=ax, label=row['start_date_str'][0])
            aoi.exterior.plot(color='black', ax=ax, label='AOI')
            plt.legend()

    ax.set_title(ptype.upper())


def plot_network_graph(df: gpd.GeoDataFrame) -> list:
    # Get unique dates
    df['reference_date'] = pd.to_datetime(df['reference_date'])
    df['secondary_date'] = pd.to_datetime(df['secondary_date'])
    
    unique_dates = df.reference_date.tolist() + df.secondary_date.tolist()
    unique_dates = sorted(list(set(unique_dates)))
    
    # initiate and plot date notes
    date2node = {date: k for (k, date) in enumerate(unique_dates)}
    node2date = {k: date for (date, k) in date2node.items()}
    
    # connectivity network Directed Graph
    G = nx.DiGraph()
    edges = [(date2node[ref_date], date2node[sec_date]) 
             for (ref_date, sec_date) in zip(df.reference_date, df.secondary_date)]
    G.add_edges_from(edges)
    nx.draw(G)
    
    print('Network connected:', '\033[1m', nx.has_path(G, target=date2node[unique_dates[0]],
                                              source=date2node[unique_dates[-1]]),'\033[0m')
    
    # Time Series Graph
    fig, ax = plt.subplots(figsize=(15, 5))

    increment = [date.month + date.day for date in unique_dates]

    # source: https://stackoverflow.com/a/27852570
    scat = ax.scatter(unique_dates, increment)
    position = scat.get_offsets().data

    pos = {date2node[date]: position[k] for (k, date) in enumerate(unique_dates)}
    nx.draw_networkx_edges(G, pos=pos, ax=ax)
    ax.grid('on')
    ax.tick_params(axis='x',
                   which='major',
                   labelbottom=True,
                   labelleft=True)
    ymin, ymax = ax.get_ylim()
    plt.show

    return unique_dates