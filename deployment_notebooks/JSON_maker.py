#!/usr/bin/env python3
"""Make GeoJSON from KMZ files.

Copyright 2019, by the California Institute of Technology.
ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
Author(s): Simran Sangha & Emre Havazli
"""

import argparse
import glob
import logging
import os
import shutil
from pathlib import Path
from zipfile import ZipFile

import fiona
import geopandas as gpd
import pandas as pd
from rasterio.crs import CRS

fiona.drvsupport.supported_drivers['kml'] = 'rw' # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers['KML'] = 'rw' # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers['libkml'] = 'rw' # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw' # enable KML support which is disabled by default

LOGGER = logging.getLogger('JSON_maker.py')

def create_parser():
    # parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Convert KMZ to GeoJSON')
    parser.add_argument('kmz_dir', help='input directory')
    parser.add_argument('-o', '--output_dir', dest='output_dir',
                        default='./aoi', help='output directory')
    parser.add_argument('-w', '--write_file', dest='write_file',
                        default=False, help='Write out GeoJSON file True/False')
    parser.add_argument('-mdb', '--min_days_back',
                        dest='min_days_back', default=[0],
                        help='minimum days backward. default: 0')
    parser.add_argument('-mrl', '--month_range_lower',
                        dest='month_range_lower', default=1,
                        help='month range lower. default: 1')
    parser.add_argument('-mru', '--month_range_upper',
                        dest='month_range_upper', default=12,
                        help='month range upper. default: 12')
    parser.add_argument('-n', '--n_neighbors',
                        dest='n_neighbors', default=3,
                        help='number of neighbors. default: 3')

    return parser

def cmd_parse():
    parser = create_parser()
    args = parser.parse_args()
    args.kmz_dir = os.path.abspath(args.kmz_dir)

    kwargs = {'min_days_backward': args.min_days_back,
              'month_range_lower': args.month_range_lower,
              'month_range_upper': args.month_range_upper,
              'num_neighbors': args.n_neighbors}

    return args, kwargs


def read_one(kmz_path, output_dir, **kwargs):
    kmz_zip = ZipFile(kmz_path, 'r')
    temp_dir = Path(output_dir+'/'+'tmp_{kmz_path.stem}')
    temp_dir.mkdir(exist_ok=True)
    kml_path = kmz_zip.extract('doc.kml', temp_dir)
    df = gpd.read_file(kml_path)

    df = gpd.GeoDataFrame(geometry=[df.geometry.unary_union],
                          crs=CRS.from_epsg(4326))
    n = len(kwargs['min_days_backward']) - 1
    if n:
        df = df.append([df]*n).reset_index(drop=True)
    
    for key in list(kwargs):
        df[key] = kwargs[key]
        
    tokens = str(Path(kmz_path).stem).split('_')
    df['aoi_name'] = tokens[-1]
    df['path_number'] = int(tokens[1])
    
    columns = ['aoi_name','path_number'] + list(kwargs.keys()) + ['geometry']
    df = df[columns]
    shutil.rmtree(temp_dir)

    return df

def write_one(df, output_dir, write_file):
    output_dir = Path(output_dir)
    path_number = df.path_number.tolist()[0]
    aoi_name = df.aoi_name[0]

    if write_file:
        out_path = output_dir/f'{aoi_name}_pathNumber{path_number}.geojson'
        LOGGER.info(out_path)
        df.to_file(out_path, driver='GeoJSON')
    else:
        df_plot = df.exterior.plot()
        fig = df_plot.get_figure()
        out_path = output_dir/f'{aoi_name}_pathNumber{path_number}.png'
        fig.savefig(out_path, dpi=300, facecolor='white')

    return out_path

def init_logger(output_dir, log_level):
    """Initiate logger."""
    log_path = Path(os.path.abspath(os.path.join(output_dir, 'logs')))
    log_path.mkdir(parents=True, exist_ok=True)
    level = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR}[log_level]

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=level, format=log_format,
        handlers=[
            logging.StreamHandler(), logging.FileHandler(
                os.path.join(log_path, 'JSON_maker.log'), mode='w')])

def main():
    args, kwargs = cmd_parse()
    init_logger(args.output_dir, 'info')

    LOGGER.info('Parameters in use: ')
    for key, value in args.__dict__.items():
        LOGGER.info('%s: %s', key, value)

    for key, value in kwargs.items():
        LOGGER.info('%s: %s', key, value)

    kmz_files = glob.glob(os.path.join(args.kmz_dir, '*.kmz'))
    LOGGER.info('Following kmz files found: {}'.format(kmz_files))
    
    for i in kmz_files:
        read_one_p = read_one(i, args.output_dir, **kwargs)
        out_path = write_one(read_one_p, args.output_dir, args.write_file)
        LOGGER.info('Created file: {}'.format(out_path))


if __name__ == '__main__':
    main()
