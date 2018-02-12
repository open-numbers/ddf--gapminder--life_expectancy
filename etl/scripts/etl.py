# -*- coding: utf-8 -*-

''' ETL script of gapminder--life_expectancy
'''

import os

from ddf_utils.chef.api import Chef

recipe_file = '../recipes/etl.yml'

if __name__ == '__main__':
    chef = Chef.from_recipe(recipe_file)

    try:
        d = os.environ['DATASETS_DIR']
        chef.add_config(ddf_dir=d)
    except KeyError:
        pass

    chef.run(serve=True, outpath='../../')
