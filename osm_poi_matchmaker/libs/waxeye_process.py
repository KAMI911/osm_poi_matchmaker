# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import traceback
    from osm_poi_matchmaker.libs.waxeye import AST
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def waxeye_process(ast_items):
    try:
        if isinstance(ast_items, Exception):  # For example ParseError
            logging.error("ParseError detected: %s", ast_items)
            return None
        processed = dict()
        for index, ast_item in enumerate(getattr(ast_items, 'children', [])):  # Children must exits here
            if hasattr(ast_item, 'children'):
                try:
                    stri = 0
                    if isinstance(ast_item.children[0], AST) and ast_item.children[0] != []:
                        sub_processed = waxeye_process(ast_item)
                        if processed is not None:
                            processed.update(sub_processed)
                        else:
                            processed = sub_processed
                    else:
                        string_value = ''
                        string_type = ast_item.type
                        for stri in range(len(ast_item.children)):
                            check_string = ast_item.children[stri]
                            if isinstance(check_string, str):
                                string_value += ast_item.children[stri]
                            if isinstance(check_string, AST):
                                processed[check_string.type] = ''.join(check_string.children)
                        processed[string_type] = string_value
                except IndexError:
                    logging.warning("IndexError occurred while processing AST node: %s", ast_item)
                    continue
            else:
                processed[ast_item.type] = ''.join(ast_item)
        return processed
    except Exception as err_waxeye:
        logging.exception('Exception occurred: {}'.format(err_waxeye))
        logging.exception(traceback.format_exc())
        logging.debug(ast_items)
        return None
