try:
    import logging
    import sys
    import importlib
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception("Exception occurred")
    sys.exit(128)


def import_module(module_name, class_name):
    module = importlib.import_module(module_name)
    load_class = getattr(module, class_name)
    return load_class
