try:
    import logging
    import sys
    import importlib
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def import_module(module_name, class_name):
    """Dynamically import a data provider module and return its class object.

    Used by import_poi_data_module() to load a provider by its config-file name
    (e.g. 'hu_aldi') without a static import for every provider.

    Args:
        module_name (str): Fully qualified module path, e.g. 'dataproviders.hu_aldi'.
        class_name (str): Name of the class to fetch from that module, e.g. 'hu_aldi'.

    Returns:
        type: The class object, ready to be instantiated by the caller.
    """
    module = importlib.import_module(module_name)
    load_class = getattr(module, class_name)
    return load_class
