try:
    import traceback
    import logging
    import sys
    import importlib
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    sys.exit(128)


def import_module(module_name, class_name):
    module = importlib.import_module(module_name)
    load_class = getattr(module, class_name)
    return load_class
