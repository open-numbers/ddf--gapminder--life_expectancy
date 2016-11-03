# -*- coding: utf-8 -*-

''' ETL script of gapminder--life_expectancy
'''

import pandas as pd
import os
from ddf_utils.str import to_concept_id
from ddf_utils.index import create_index_file
from ddf_utils import ddf_reader as dr
from ddf_utils.io import cleanup

source = '../source/indicator life_expectancy_at_birth.xlsx'
out_dir = '../../'
dr.SEARCH_PATH = '../../../'


def run_etl(source_file):
    data = pd.read_excel(source_file)

    # grab the concept name
    # the first column is the concept name in this source file.
    cname = data.columns[0]
    cid = to_concept_id(cname)

    data = data.set_index(cname)
    data.index.name = 'geo'

    # make all index in columns
    data = data.stack().reset_index()

    data.columns = ['geo', 'time', cid]

    # align geo to gapminder's geo domain
    geo_upper = data.geo.unique()
    geo = dr.ddf_entities('ddf--gapminder--geo_entity_domain')
    country = geo['country']

    mapping = {}

    for g in geo_upper:
        m0 = country['name'] == g
        m1 = country['gapminder_list'] == g

        m = m0 | m1
        filtered = country[m]
        if len(filtered) > 0:
            mapping[g] = filtered['country'].values[0]
        else:
            print('not found: ', g)

    data.geo = data.geo.map(lambda x: mapping[x])
    path = os.path.join(out_dir, 'ddf--datapoints--{}--by--geo--time.csv'.format(cid))
    data.to_csv(path, index=False, float_format='%.15g')

    # concepts
    concepts = ['Name', 'Time', cname, 'Country', 'Indicator URL']
    ids = ['name', 'time', cid, 'geo', 'indicator_url']

    cdf = pd.DataFrame({'concept': ids, 'name': concepts})

    cdf['concept_type'] = 'string'
    cdf.loc[1, 'concept_type'] = 'time'
    cdf.loc[2, 'concept_type'] = 'measure'
    cdf.loc[3, 'concept_type'] = 'entity_domain'

    cdf.loc[2, 'indicator_url'] = 'https://github.com/open-numbers/ddf--gapminder--life_expectancy'

    cdf.to_csv(os.path.join(out_dir, 'ddf--concepts.csv'), index=False)

    # geo entities
    geo_df = pd.DataFrame.from_records(mapping, index=[0])
    geo_df = geo_df.T
    geo_df = geo_df.reset_index()
    geo_df.columns = ['name', 'geo']
    geo_df.to_csv(os.path.join(out_dir, 'ddf--entities--geo.csv'), index=False)


if __name__ == '__main__':
    print('cleanup old data...')
    cleanup(out_dir)
    print('creating ddf files...')
    run_etl(source)
    print('adding index file...')
    create_index_file(out_dir)
    print('Done.')
