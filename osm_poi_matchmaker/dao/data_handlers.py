# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import hashlib
    import traceback
    from osm_poi_matchmaker.dao.data_structure import City, POI_common, POI_address, POI_address_raw, POI_patch, Street_type, Country
    from osm_poi_matchmaker.libs import address
    from osm_poi_matchmaker.dao import poi_array_structure
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

POI_COLS = poi_array_structure.POI_COLS
POI_COLS_RAW = poi_array_structure.POI_COLS_RAW


def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        logging.debug('Already added: %s', instance)
        return instance
    else:
        try:
            instance = model(**kwargs)
            session.add(instance)
            return instance
        except Exception as e:
            logging.error('Cannot add to the database. (%s)', e)
            logging.exception('Exception occurred')
            raise e


def get_or_create_poi(session, model, **kwargs):
    if kwargs['poi_common_id'] is not None:
        if kwargs.get('poi_common_id') is not None and kwargs.get('poi_addr_city') is not None and (
        ( kwargs.get('poi_addr_street') is not None and kwargs.get('poi_addr_housenumber') is not None) or (
        kwargs.get('poi_conscriptionnumber') is not None)):
            logging.debug('Fully filled basic data record')
        else:
            logging.warning('Missing record data: %s', kwargs)
    instance = session.query(model) \
        .filter_by(poi_common_id=kwargs.get('poi_common_id')) \
        .filter_by(poi_addr_city=kwargs.get('poi_addr_city')) \
        .filter_by(poi_addr_street=kwargs.get('poi_addr_street')) \
        .filter_by(poi_addr_housenumber=kwargs.get('poi_addr_housenumber')) \
        .filter_by(poi_conscriptionnumber=kwargs.get('poi_conscriptionnumber')) \
        .filter_by(poi_branch=kwargs.get('poi_branch')) \
        .first()
    if instance:
        logging.debug('Already added: %s', instance)
        return instance
    else:
        try:
            instance = model(**kwargs)
            session.add(instance)
            return instance
        except Exception as e:
            logging.error('Cannot add to the database. (%s)', e)
            logging.exception('Exception occurred')
            raise e

def search_poi_patch(session, model, **kwargs):
    if kwargs.get('poi_common_id') == "*":
        kwargs['valami'] == "%"
    if kwargs.get('poi_common_id') == "*":
        kwargs['valami'] == "%"
    if kwargs.get('poi_common_id') == "*":
        kwargs['valami'] == "%"

    instance = session.query(model) \
        .filter_by(poi_common_id=kwargs['poi_common_id']) \
        .filter_by(poi_addr_city=kwargs['poi_addr_city']) \
        .filter_by(poi_addr_street=kwargs['poi_addr_street']) \
        .filter_by(poi_addr_housenumber=kwargs['poi_addr_housenumber']) \
        .filter_by(poi_conscriptionnumber=kwargs['poi_conscriptionnumber']) \
        .filter_by(poi_branch=kwargs['poi_branch']) \
        .first()
    if instance:
        logging.debug('Already added: %s', instance)
        return instance
    else:
        try:
            instance = model(**kwargs)
            session.add(instance)
            return instance
        except Exception as e:
            logging.error('Cannot add to the database. (%s)', e)
            logging.exception('Exception occurred')
            raise e


def get_or_create_cache(session, model, **kwargs):
    if kwargs.get('osm_id') is not None and kwargs.get('osm_object_type'):
        instance = session.query(model) \
            .filter_by(osm_id=kwargs.get('osm_id')).filter_by(osm_object_type=kwargs.get('poi_addr_city')).first()
    if instance:
        logging.debug('Already added: %s', instance)
        return instance
    else:
        try:
            instance = model(**kwargs)
            session.add(instance)
            return instance
        except Exception as e:
            logging.error('Cannot add to the database. (%s)', e)
            logging.exception('Exception occurred')
            raise e

def get_or_create_common(session, model, **kwargs):
    if kwargs['poi_code'] is not None and kwargs['poi_code'] != '':
        instance = session.query(model).filter_by(poi_code=kwargs['poi_code']).first()
    if instance:
        logging.debug('Already added: %s', instance)
        return instance
    else:
        try:
            instance = model(**kwargs)
            session.add(instance)
            return instance
        except Exception as e:
            logging.error('Cannot add to the database. (%s)', e)
            logging.exception('Exception occurred')
            raise e

def insert_city_dataframe(session, city_df):
    city_df.columns = ['city_post_code', 'city_name']
    try:
        for index, city_data in city_df.iterrows():
            get_or_create(session, City, city_post_code=city_data['city_post_code'],
                          city_name=address.clean_city(city_data['city_name']))
    except Exception as e:

        logging.error('Rolled back: %s.', e)
        logging.error(city_data)
        logging.exception('Exception occurred')

        session.rollback()
    else:
        logging.info('Successfully added %s city items to the dataset.', len(city_df))
        session.commit()
    finally:
        session.close()


def insert_street_type_dataframe(session, city_df):
    city_df.columns = ['street_type']
    try:
        for index, city_data in city_df.iterrows():
            get_or_create(session, Street_type, street_type=city_data['street_type'])
    except Exception as e:
        logging.error('Rolled back: %s.', e)
        logging.error(city_data)
        logging.exception('Exception occurred')

        session.rollback()
    else:
        logging.info('Successfully added %s street type items to the dataset.', len(city_df))
        session.commit()
    finally:
        session.close()

def insert_patch_data_dataframe(session, patch_df):
    patch_df.columns = ['poi_code', 'orig_postcode', 'orig_city', 'orig_street', 'orig_housenumber', 'orig_conscriptionnumber', 'orig_name','new_postcode', 'new_city', 'new_street', 'new_housenumber', 'new_conscriptionnumber', 'new_name']
    try:
        for index, patch_data in patch_df.iterrows():
            get_or_create(session, POI_patch, poi_code=patch_data['poi_code'], orig_postcode=patch_data['orig_postcode'], orig_city=patch_data['orig_city'],
            orig_street=patch_data['orig_street'], orig_housenumber=patch_data['orig_housenumber'], orig_conscriptionnumber=patch_data['orig_conscriptionnumber'],
            orig_name=patch_data['orig_name'], new_postcode=patch_data['new_postcode'], new_city=patch_data['new_city'],
            new_street=patch_data['new_street'], new_housenumber=patch_data['new_housenumber'], new_conscriptionnumber=patch_data['new_conscriptionnumber'],
            new_name=patch_data['new_name'])
    except Exception as e:
        logging.error('Rolled back: %s.', e)
        logging.error(patch_data)
        logging.exception('Exception occurred')

        session.rollback()
    else:
        logging.info('Successfully added %s patch items to the dataset.', len(patch_df))
        session.commit()
    finally:
        session.close()

def insert_country_data_dataframe(session, country_df):
    country_df.columns = ['country_code','continent_code','country_name','country_iso3','country_number','country_full_name']
    try:
        for index, country_data in country_df.iterrows():
            get_or_create(session, Country, country_code=country_data['country_code'],continent_code=country_data['continent_code'],country_name=country_data['country_name'],
            country_iso3=country_data['country_iso3'],country_number=country_data['country_number'],country_full_name=country_data['country_full_name'])

    except Exception as e:
        logging.error('Rolled back: %s.', e)
        logging.error(country_data)
        logging.exception('Exception occurred')

        session.rollback()
    else:
        logging.info('Successfully added %s country items to the dataset.', len(country_df))
        session.commit()
    finally:
        session.close()

def insert_common_dataframe(session, common_df):
    common_df.columns = ['poi_name', 'poi_tags', 'poi_url_base', 'poi_code']
    try:
        for index, poi_common_data in common_df.iterrows():
            get_or_create_common(session, POI_common, **poi_common_data)
    except Exception as e:
        logging.error('Rolled back: %s.', e)
        logging.error(poi_common_data)
        logging.exception('Exception occurred')

        session.rollback()
    else:
        logging.info('Successfully added %s common items to the dataset.', len(common_df))
        session.commit()
    finally:
        session.close()


def search_for_postcode(session, city_name):
    city_col = session.query(City.city_post_code).filter(City.city_name == city_name).all()
    if len(city_col) == 1:
        return city_col
    else:
        logging.info('Cannot determine the post code from city name (%s).', city_name)
        return None


def insert_poi_dataframe(session, poi_df, raw = True):
    if raw is True:
        poi_df.columns = POI_COLS_RAW
    poi_df[['poi_postcode']] = poi_df[['poi_postcode']].fillna('0000')
    poi_df[['poi_postcode']] = poi_df[['poi_postcode']].astype('int32')
    poi_dict = poi_df.to_dict('records')
    try:
        for poi_data in poi_dict:
            city_col = session.query(City.city_id) \
              .filter(City.city_name == poi_data.get('poi_city')) \
              .filter(City.city_post_code == poi_data.get('poi_postcode')).first()
            common_col = session.query(POI_common.pc_id) \
              .filter(POI_common.poi_code == poi_data.get('poi_code')).first()
            if city_col is not None:
                poi_data['poi_addr_city'] = city_col[0]
            if common_col is not None:
                poi_data['poi_common_id'] = common_col[0]
            if 'poi_name' in poi_data: del poi_data['poi_name']
            if 'poi_code' in poi_data: del poi_data['poi_code']
            if raw is True:
                get_or_create_poi(session, POI_address_raw, **poi_data)
            else:
                get_or_create_poi(session, POI_address, **poi_data)
    except Exception as e:
        logging.exception('Exception occured: {} rolled back: {}'.format(e, traceback.print_exc()))
        session.rollback()
        raise e
    else:
        try:
            session.commit()
            logging.info('Successfully added %s POI items to the dataset.', len(poi_dict))
        except Exception as e:
            logging.exception('Exception occured: {} unsuccessfull commit: {}'.format(e, traceback.print_exc()))
            session.rollback()
        finally:
            session.close()
    finally:
        session.close()


def insert_type(session, type_data):
    try:
        for i in type_data:
            get_or_create_common(session, POI_common, **i)
    except Exception as e:
        logging.error('Rolled back: %s.', e)
        logging.error(i)
        logging.exception('Exception occurred')

        session.rollback()
    else:
        logging.info('Successfully added %s type items to the dataset.', len(type_data))
        session.commit()
    finally:
        session.close()


def insert(session, **kwargs):
    try:
        city_col = session.query(City.city_id).filter(City.city_name == kwargs['poi_city']).filter(
            City.city_post_code == kwargs['poi_postcode']).first()
        common_col = session.query(POI_common.pc_id).filter(POI_common.poi_code == kwargs['poi_code']).first()
        kwargs['poi_addr_city'] = city_col
        kwargs['poi_common_id'] = common_col
        kwargs['poi_hash'] = hashlib.sha512(
            '{}{}{}{}{}{}'.format(kwargs['poi_code'], kwargs['poi_postcode'], kwargs['poi_city'],
                                  kwargs['poi_addr_street'], kwargs['poi_addr_housenumber'],
                                  kwargs['poi_conscriptionnumber']).lower().replace(' ', '').encode(
                'utf-8')).hexdigest()
        if 'poi_name' in kwargs: del kwargs['poi_name']
        if 'poi_code' in kwargs: del kwargs['poi_code']
        get_or_create_poi(session, POI_address, **kwargs)
    except Exception as e:
        logging.error('Rolled back: %s.', e)
        logging.error(kwargs)
        logging.exception('Exception occurred')

        session.rollback()
    else:
        logging.debug('Successfully added the item to the dataset.')
        session.commit()
    finally:
        session.close()
