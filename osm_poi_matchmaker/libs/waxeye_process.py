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
        print('start')
        processed = dict()
        for index, ast_item in enumerate(ast_items.children):
            print('main')
            print(ast_item)
            if hasattr(ast_item, 'children'):
                print(ast_item.children)
                try:
                    stri = 0
                    if isinstance(ast_item.children[0], AST) and ast_item.children[0] != []:
                        sub_processed = waxeye_process(ast_item)
                        if processed is not None:
                            processed.update(sub_processed)
                        else:
                            processed = sub_processed
                    else:
                        for stri in range(len(ast_item.children)):
                            check_string = ast_item.children[stri]
                            if isinstance(check_string, str):
                                continue
                            else:
                                print(stri, check_string)
                                sub_processed = waxeye_process(check_string)
                                processed.update(sub_processed)
                                print(key, value)
                        value = ''.join(ast_item.children[0:stri+1])
                        key = ast_item.type
                        processed[key] = value
                        print(key, value)
                except IndexError:
                    continue
            else:
                print(ast_item)
                value = ''.join(ast_item)
                key = ast_item.type
                processed[key] = value
                print(key, value)
        print(processed)
        return processed
    except Exception as err_waxeye:
        logging.exception('Exception occurred: {}'.format(err_waxeye))
        logging.error(traceback.print_exc())
        return None
