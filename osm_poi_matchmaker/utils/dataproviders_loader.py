try:
    import traceback
    import logging
    import sys
    import importlib
except ImportError as err:
    logging.error('Error {error} import module: {module}', module=__name__, error=err)
    logging.error(traceback.print_exc())
    sys.exit(128)


def import_module(module_name, class_name):
    module = importlib.import_module(module_name)
    load_class = getattr(module, class_name)
    return load_class
