import json

COLLECTION_CONCEPT_ID = 'C1595422627-ASF'
CMR_URL = 'https://cmr.earthdata.nasa.gov/search/granules.echo10'

# Deduplication Pt. 1
def get_gunw_hash_id(reference_ids: list, secondary_ids: list) -> str:
    import hashlib
    all_ids = json.dumps([' '.join(sorted(reference_ids)),
                          ' '.join(sorted(secondary_ids))
                          ]).encode('utf8')
    hash_id = hashlib.md5(all_ids).hexdigest()
    return hash_id

def hasher(row):
    return get_gunw_hash_id(row['reference'], row['secondary'])

# Deduplication Pt. 2
def parse_echo10(echo10_xml: str):
    import xml.etree.ElementTree as ET
    granules = []
    root = ET.fromstring(echo10_xml)
    for granule in root.findall('result/Granule'):
        g = {
            'product_id': granule.find('GranuleUR').text,
            'product_version': granule.find('GranuleUR').text.split('-')[-1],
            'reference_scenes': [],
            'secondary_scenes': []
        }
        for input_granule in granule.findall('InputGranules/InputGranule'):
            input_granule_type, input_granule_name = input_granule.text.split(' ')
            if input_granule_type == '[Reference]':
                g['reference_scenes'].append(input_granule_name)
            else:
                g['secondary_scenes'].append(input_granule_name)
        granules.append(g)
    return granules

def get_cmr_products(path: int = None):
    import requests
    session = requests.Session()
    search_params = {
        'provider': 'ASF',
        'collection_concept_id': COLLECTION_CONCEPT_ID,
        'page_size': 2000,
    }
    if path is not None:
        search_params['attribute[]'] = f'int,PATH_NUMBER,{path}'
    headers = {}
    products = []

    while True:
        response = session.get(CMR_URL, params=search_params, headers=headers)
        response.raise_for_status()

        parsed_results = parse_echo10(response.text)
        products.extend(parsed_results)

        if 'CMR-Search-After' not in response.headers:
            break
        headers = {'CMR-Search-After': response.headers['CMR-Search-After']}

    return products

def capture_cmr_products(row, cmr_products):
    import numpy as np
    '''Capture products that exist in CMR based on reference and secondary scenes'''
    # concatenate reference and secondary scene in each dataframe row
    row_scenes = row['reference'] + row['secondary']
    # flag True if scenes from enumerator results are within CMR results
    if any(set(row_scenes).issubset(set(x)) for x in cmr_products):
        product_on_cmr = True
    else:
        product_on_cmr = np.nan
    return product_on_cmr