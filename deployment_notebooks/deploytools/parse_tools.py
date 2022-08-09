import pandas as pd
import numpy as np


def shapefile_area(file_bbox,
        bounds = False):
    """Compute km\u00b2 area of shapefile."""
    # import dependencies
    from pyproj import Proj
    from shapely.geometry import shape

    # loop through polygons
    shape_area = 0
    # pass single polygon as list
    if file_bbox.type == 'Polygon': file_bbox = [file_bbox]
    for polyobj in file_bbox:
        #first check if empty
        if polyobj.is_empty:
            shape_area += 0
            continue
        # get coords
        if bounds:
            # Pass coordinates of bounds as opposed to cutline
            # Necessary for estimating DEM/mask footprints
            WSEN = polyobj.bounds
            lon = np.array([WSEN[0],WSEN[0],WSEN[2],WSEN[2],WSEN[0]])
            lat = np.array([WSEN[1],WSEN[3],WSEN[3],WSEN[1],WSEN[1]])
        else:
            lon, lat = polyobj.exterior.coords.xy

        # use equal area projection centered on/bracketing AOI
        pa = Proj("+proj=aea +lat_1={} +lat_2={} +lat_0={} +lon_0={}". \
             format(min(lat), max(lat), (max(lat)+min(lat))/2, \
             (max(lon)+min(lon))/2))
        x, y = pa(lon, lat)
        cop = {"type": "Polygon", "coordinates": [zip(x, y)]}
        shape_area += shape(cop).area/1e6  # area in km^2

    return shape_area
    

def continuous_time(product_df, iter_id='fileID'):
    """
    Split the products into spatiotemporally continuous groups.
    Split products by individual, continuous interferograms.
    Input must be already sorted by pair and start-time to fit
    the logic scheme below.
    Using their time-tags, this function determines whether or not
    successive products are in the same orbit.
    If in the same orbit, the program determines whether or not they
    overlap in time and are therefore spatially contiguous,
    and rejects/reports cases for which there is no temporal overlap
    and therefore a spatial gap.
    """
    # import dependencies
    from datetime import datetime, timedelta
    from shapely.ops import unary_union

    # pass scenes that have no gaps
    sorted_products = []
    track_rejected_inds = []
    pair_dict = {}
    product_df_dict = product_df.to_dict('records')
    # Check for (and remove) duplicate products
    # If multiple pairs in list, cycle through
    # and evaluate temporal connectivity.
    if len(product_df_dict)==1:
        scene = product_df_dict[0]
        dict_1 = package_dict(scene, scene, 0)
        dict_2 = package_dict(scene, scene, 1)
        new_dict = [dict_1, dict_2]
        sorted_products.extend([new_dict])

    # If multiple pairs in list
    # cycle through and evaluate temporal connectivity
    for i in enumerate(product_df_dict[:-1]):
        scene = i[1]
        new_scene = product_df_dict[i[0]+1]
        scene_ind = scene['ind_col']
        scene_t = datetime.strptime( \
            scene['fileID'][17:25], "%Y%m%d")
        scene_area = scene['geometry']
        new_scene_ind = new_scene['ind_col']
        new_scene_t = datetime.strptime( \
            new_scene['fileID'][17:25], "%Y%m%d")
        new_scene_area = new_scene['geometry']

        # Only pass scene if it temporally (i.e. in same orbit)
        # and spatially overlaps with reference scene
        if scene_area.intersection(new_scene_area).area > 0. and \
                abs(new_scene_t-scene_t) <= timedelta(days=1):
            # Do not export prod if already tracked as a rejected pair
            if scene_ind in track_rejected_inds or \
                    new_scene_ind in track_rejected_inds:
                track_rejected_inds.extend((scene_ind, \
                    new_scene_ind))
                continue
            # Check if IFG dict corresponding to ref prod already exists
            # and if it does then append values
            try:
                dict_ind = sorted_products.index(next(item for item \
                        in sorted_products if i[1][iter_id] \
                        in item[iter_id]))
                sorted_products[dict_ind] = {key: np.hstack([value] + \
                        [product_df_dict[i[0]+1][key]]).tolist() \
                        for key, value in sorted_products[dict_ind].items()}
            # Match corresponding to scene NOT found,
            # so initialize dictionary for new scene
            except:
                sorted_products.extend([dict(zip(i[1].keys(), \
                        [list(a) for a in zip(i[1].values(), \
                        product_df_dict[i[0]+1].values())]))])
                
        # If pairs are within the same day
        # but do not intersect this means there is a gap
        # Reject date from prod list, and keep track of all failed dates
        elif scene_area.intersection(new_scene_area).area == 0. and \
                abs(new_scene_t-scene_t) <= timedelta(days=1):
            track_rejected_inds.extend((scene_ind, \
                new_scene_ind))
            print("Gap for scene {} \n".format(scene_ind))

        # If prods correspond to different orbits entirely
        else:
            # Check if scene dict corresponding to ref prod already exists
            # and if it does not then pass as new scene
            if [item for item in sorted_products if i[1][iter_id] in \
                    item[iter_id]]==[] and scene_ind not in \
                    track_rejected_inds:
                sorted_products.extend([dict(zip(i[1].keys(), \
                        [list(a) for a in zip(i[1].values())]))])
            # Check if scene dict corresponding to ref prod already exists
            # and if it does not then pass as new scene
            if [item for item in sorted_products if \
                    product_df_dict[i[0]+1][iter_id] in item[iter_id]]==[] \
                    and new_scene_ind not in track_rejected_inds:
                sorted_products.extend([dict(zip( \
                        product_df_dict[i[0]+1].keys(), \
                        [list(a) for a in \
                        zip(product_df_dict[i[0]+1].values())]))])

    # Remove duplicate dates
    track_rejected_inds=list(set(track_rejected_inds))
    if len(track_rejected_inds)>0:
        print('{} out of {} scenes rejected since '
              'stitched scenes would have gaps'.format( \
              len(track_rejected_inds), \
              len(sorted_products)))
        # Provide report of which files were kept vs. which were not
        print('Specifically, gaps were found between the '
              'following scenes:')
        record_rejected_scenes = []
        for item in sorted_products:
            if item['ind_col'] in track_rejected_inds:
                record_rejected_scenes.extend(item['fileID'][17:25])
        record_rejected_scenes = list(set(record_rejected_scenes))
        for i in record_rejected_scenes:
            print(i)
    else:
        print('All {} scenes are spatially continuous.'.format( \
               len(sorted_products)))

    # pass scenes that have no gaps
    sorted_products = [item for item in sorted_products \
            if not (any(x in track_rejected_inds for x in item['ind_col']))]

    # Report dictionaries for all valid products
    if sorted_products == []: #Check if pairs were successfully selected
        raise Exception('No scenes meet spatial criteria '
                        'due to gaps and/or invalid input. '
                        'Nothing to export.')

    # Combine polygons
    for i in enumerate(sorted_products):
        sorted_products[i[0]]['geometry'] = unary_union(i[1]['geometry'])

    # combine and record scenes with gaps
    track_kept_inds = pd.DataFrame(sorted_products)['ind_col'].to_list()
    track_kept_inds = [item for sublist in track_kept_inds for item in sublist]
    temp_gap_scenes_dict = [item for item in product_df_dict \
            if not item['ind_col'] in track_kept_inds]
    gap_scenes_dict = []
    for i in enumerate(temp_gap_scenes_dict[:-1]):
        # Parse the first frame's metadata
        first_frame_ind = i[1]['ind_col']
        first_frame = datetime.strptime( \
                i[1]['fileID'][17:25], "%Y%m%d")
        # Parse the second frame's metadata
        next_frame_ind = temp_gap_scenes_dict[i[0]+1]['ind_col']
        next_frame = datetime.strptime( \
                temp_gap_scenes_dict[i[0]+1]['fileID'][17:25], "%Y%m%d")
        # Determine if next product in time is in same orbit
        # If it is within same orbit cycle, try to append scene.
        # This accounts for day change.
        if abs(next_frame-first_frame) <= \
            timedelta(days=1):
            # Check if dictionary for scene already exists,
            # and if it does then append values
            try:
                dict_ind = gap_scenes_dict.index(next(item for item \
                        in gap_scenes_dict if i[1][iter_id] \
                        in item[iter_id]))
                gap_scenes_dict[dict_ind] = {key: np.hstack([value] + \
                        [temp_gap_scenes_dict[i[0]+1][key]]).tolist() \
                        for key, value in gap_scenes_dict[dict_ind].items()}
            # Match corresponding to scene NOT found,
            # so initialize dictionary for new scene
            except:
                gap_scenes_dict.extend([dict(zip(i[1].keys(), \
                        [list(a) for a in zip(i[1].values(), \
                        temp_gap_scenes_dict[i[0]+1].values())]))])
        # Products correspond to different dates,
        # So pass both as separate scenes.
        else:
            # Check if dictionary for corresponding scene already exists.
            if [item for item in gap_scenes_dict if i[1][iter_id] in \
                    item[iter_id]]==[]:
                gap_scenes_dict.extend([dict(zip(i[1].keys(), \
                        [list(a) for a in zip(i[1].values())]))])
            # Initiate new scene
            if [item for item in gap_scenes_dict if \
                    temp_gap_scenes_dict[i[0]+1][iter_id] in item[iter_id]]==[]:
                gap_scenes_dict.extend([dict(zip( \
                        temp_gap_scenes_dict[i[0]+1].keys(), \
                        [list(a) for a in \
                        zip(temp_gap_scenes_dict[i[0]+1].values())]))])

    # there may be some extra missed pairs with gaps
    if gap_scenes_dict != []:
        extra_track_rejected_inds = pd.DataFrame(gap_scenes_dict)['ind_col'].to_list()
        extra_track_rejected_inds = [item for sublist in extra_track_rejected_inds for item in sublist]
        track_rejected_inds.extend(extra_track_rejected_inds)

    return sorted_products, track_rejected_inds, gap_scenes_dict
    

def minimum_overlap_query(tiles,
        aoi,
        azimuth_mismatch=0.01,
        iter_id='fileID'):
    """
    Master function managing checks for SAR scene spatiotemporal contiguity
    and filtering out scenes based off of user-defined spatial coverage threshold
    """
    # initiate dataframe
    tiles = tiles.sort_values(['startTime'])
    updated_tiles = tiles.copy()

    # Drop scenes that don't intersect with AOI at all
    orig_len = updated_tiles.shape[0]
    for index, row in tiles.iterrows():
        intersection_area = aoi.intersection(row['geometry'])
        overlap_area = shapefile_area(intersection_area)
        aoi_area = shapefile_area(aoi)
        percentage_coverage = (overlap_area/aoi_area)*100
        if percentage_coverage == 0:
            drop_ind = updated_tiles[updated_tiles['fileID'] == row['fileID']].index
            updated_tiles = updated_tiles.drop(index=drop_ind)
    updated_tiles = updated_tiles.reset_index(drop=True)
    print("{}/{} scenes rejected for not intersecting with the AOI".format( \
          orig_len-updated_tiles.shape[0], orig_len))

    # group IFGs spatiotemporally
    updated_tiles['ind_col'] = range(0, len(updated_tiles))
    updated_tiles_dict, dropped_indices, gap_scenes_dict = continuous_time(updated_tiles, iter_id)
    for i in dropped_indices:
        drop_ind = updated_tiles.index[updated_tiles['ind_col'] == i]
        updated_tiles.drop(drop_ind, inplace=True)
    updated_tiles = updated_tiles.reset_index(drop=True)
    
    # Kick out scenes that do not meet user-defined spatial threshold
    aoi_area = shapefile_area(aoi)
    orig_len = updated_tiles.shape[0]
    track_rejected_inds = []
    minimum_overlap_threshold = aoi_area - (250 * azimuth_mismatch)
    print("")
    print("AOI coverage: {}".format(aoi_area))
    print("Allowable area of miscoverage: {}".format(250 * azimuth_mismatch))
    print("minimum_overlap_threshold: {}".format(minimum_overlap_threshold))
    print("")
    if minimum_overlap_threshold < 0:
        raise Exception('WARNING: user-defined mismatch of {}km\u00b2 too large relative to specified AOI'.format(azimuth_mismatch))
    for i in enumerate(updated_tiles_dict):
        intersection_area = aoi.intersection(i[1]['geometry'])
        overlap_area = shapefile_area(intersection_area)
        # Kick out scenes below specified overlap threshold
        if minimum_overlap_threshold > overlap_area:
            for iter_ind in enumerate(i[1]['ind_col']):
                track_rejected_inds.append(iter_ind[1])
                print("Rejected scene {} has only {}km\u00b2 overlap with AOI".format( \
                    i[1]['fileID'][iter_ind[0]], int(overlap_area)))
                drop_ind = updated_tiles[updated_tiles['ind_col'] == iter_ind[1]].index
                updated_tiles = updated_tiles.drop(index=drop_ind)
    updated_tiles = updated_tiles.reset_index(drop=True)
    print("{}/{} scenes rejected for not meeting defined spatial criteria".format( \
          orig_len-updated_tiles.shape[0], orig_len))

    # record rejected scenes separately
    rejected_scenes_dict = [item for item in updated_tiles_dict \
            if (any(x in track_rejected_inds for x in item['ind_col']))]
    # pass scenes that are not tracked as rejected
    updated_tiles_dict = [item for item in updated_tiles_dict \
            if not (any(x in track_rejected_inds for x in item['ind_col']))]

    return updated_tiles, pd.DataFrame(updated_tiles_dict), pd.DataFrame(gap_scenes_dict), pd.DataFrame(rejected_scenes_dict)
    

def pair_spatial_check(tiles,
        aoi,
        azimuth_mismatch=0.01,
        iter_id='fileID'):
    """
    Santity check function to confirm selected pairs meet user-defined spatial coverage threshold
    """
    tiles['ind_col'] = range(0, len(tiles))
    tiles = tiles.drop(columns=['reference', 'secondary'])
    tiles_dict, dropped_pairs, gap_scenes_dict = continuous_time(tiles, iter_id='ind_col')

    # Kick out scenes that do not meet user-defined spatial threshold
    aoi_area = shapefile_area(aoi)
    orig_len = tiles.shape[0]
    track_rejected_inds = []
    minimum_overlap_threshold = aoi_area - (250 * azimuth_mismatch)
    if minimum_overlap_threshold < 0:
        raise Exception('WARNING: user-defined mismatch of {}km\u00b2 too large relative to specified AOI'.format(azimuth_mismatch))
    for i in enumerate(tiles_dict):
        intersection_area = aoi.intersection(i[1]['geometry'])
        overlap_area = shapefile_area(intersection_area)
        # Kick out scenes below specified overlap threshold
        if minimum_overlap_threshold > overlap_area:
            for iter_ind in enumerate(i[1]['ind_col']):
                track_rejected_inds.append(iter_ind[1])
                print("Rejected pair {} has only {}km\u00b2 overlap with AOI {}ID {}Ind".format( \
                      i[1]['reference_date'][iter_ind[0]].replace('-', '') + '_' + \
                      i[1]['secondary_date'][iter_ind[0]].replace('-', ''), \
                      overlap_area, iter_ind[1], i[0]))
                drop_ind = tiles[tiles['ind_col'] == iter_ind[1]].index
                tiles = tiles.drop(index=drop_ind)
    tiles = tiles.reset_index(drop=True)
    print("{}/{} scenes rejected for not meeting defined spatial criteria".format( \
          orig_len-tiles.shape[0], orig_len))

    # record rejected scenes separately
    rejected_scenes_dict = [item for item in tiles_dict \
            if (any(x in track_rejected_inds for x in item['ind_col']))]
    # pass scenes that are not tracked as rejected
    tiles_dict = [item for item in tiles_dict \
            if not (any(x in track_rejected_inds for x in item['ind_col']))]

    return pd.DataFrame(tiles_dict), pd.DataFrame(gap_scenes_dict), pd.DataFrame(rejected_scenes_dict)
