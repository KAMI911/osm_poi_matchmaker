
try:
    import traceback
    import logging
    import importlib
    import os
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    traceback.print_exc()
    exit(128)

def import_module(module_name, class_name):
    module = importlib.import_module(module_name)
    load_class = getattr(module, class_name)
    return load_class








