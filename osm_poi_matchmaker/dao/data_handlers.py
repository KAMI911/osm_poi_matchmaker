# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import hashlib
    import traceback
    import numpy as np
    import pandas as pd
    from osm_poi_matchmaker.dao.data_structure import City, POI_common, POI_address, POI_address_raw, POI_patch, \
        Street_type, Country
    from osm_poi_matchmaker.libs import address
    from osm_poi_matchmaker.dao import poi_array_structure
    from sqlalchemy import func
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.orm import Session
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

POI_COLS = poi_array_structure.POI_COLS
POI_COLS_RAW = poi_array_structure.POI_COLS_RAW


def count(session, model):
    return session.query(model.id).count()


def get_or_create(session: Session, model, **kwargs):
    """
    Retrieve an existing database record or create a new one if it doesn't exist.

    This function queries the database for a single instance of the given SQLAlchemy
    model that matches the provided keyword arguments. If such an instance exists, it
    is returned. If not, a new instance is created, added to the session, committed,
    and then returned.

    Args:
        session (sqlalchemy.orm.Session): The SQLAlchemy session object to use for querying and committing.
        model (Base): The SQLAlchemy model class to query or create an instance of.
        **kwargs: Arbitrary keyword arguments used to filter the query and to instantiate the model.

    Returns:
        model: An instance of the specified model, either retrieved or newly created.

    Raises:
        SQLAlchemyError: If a database error occurs during the creation or commit process.
        Exception: If any other unexpected error occurs during model instantiation or session handling.

    Logs:
        - A debug message if the instance already exists or is created.
        - An error and full traceback if an exception occurs.
    """
    try:
        instance = session.query(model).filter_by(**kwargs).first()
        if instance:
            logging.debug('Already exists: %s', instance)
            return instance

        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        logging.debug('Created new instance: %s', instance)
        return instance

    except SQLAlchemyError as e:
        session.rollback()
        logging.error('Database error: %s', e)
        logging.exception('Exception occurred during get_or_create')
        raise

    except Exception as e:
        session.rollback()
        logging.error('Unexpected error: %s', e)
        logging.exception('Exception occurred during get_or_create')
        raise


def get_or_create_poi(session, model, **kwargs):
    """
    Retrieve or create a Point of Interest (POI) record in the database.

    This function attempts to find an existing POI entry based on provided identifying
    information. If `poi_additional_ref` is present, it performs a lookup using that field.
    Otherwise, it tries to identify the POI based on address components such as city,
    street, house number, conscription number, and branch. If no matching record is found,
    it creates a new one and commits it to the database.

    Args:
        session (sqlalchemy.orm.Session): The active SQLAlchemy session for querying and committing.
        model (Base): The SQLAlchemy ORM model representing the POI table.
        **kwargs: Named keyword arguments that include fields like:
            - poi_common_id (str): Unique identifier shared across related POIs.
            - poi_addr_city (str): City name.
            - poi_addr_street (str): Street name (optional).
            - poi_addr_housenumber (str): House number (optional).
            - poi_conscriptionnumber (str): Conscription number (optional).
            - poi_branch (str): Branch identifier (optional).
            - poi_additional_ref (str): External reference (e.g., ref:bkk, ref:volanbusz).

    Returns:
        model: An existing or newly created POI instance.

    Raises:
        Exception: If the model instantiation or database commit fails.

    Logs:
        - Debug information on how the record was searched or created.
        - Warnings if required fields are missing for identification.
        - Errors and tracebacks in case of commit failures.
    """

    try:
        query = session.query(model)

        if kwargs.get('poi_additional_ref'):
            logging.debug('Search based on additional ref field.')
            query = query.filter_by(
                poi_common_id=kwargs.get('poi_common_id'),
                poi_additional_ref=kwargs.get('poi_additional_ref')
            )
        else:
            logging.warning('Missing record data: {}'.format(kwargs))
    instance = session.query(model)\
        .filter_by(poi_common_id=kwargs['poi_common_id'])\
        .filter_by(poi_addr_city=kwargs['poi_addr_city'])\
        .filter_by(poi_addr_street=kwargs['poi_addr_street'])\
        .filter_by(poi_addr_housenumber=kwargs['poi_addr_housenumber'])\
        .filter_by(poi_conscriptionnumber=kwargs['poi_conscriptionnumber'])\
        .filter_by(poi_branch=kwargs['poi_branch'])\
        .first()
    if instance:
        logging.debug('Already added: {}'.format(instance))
        return instance
    else:
        try:
            instance = model(**kwargs)
            session.add(instance)
            return instance
        except Exception as e:
            logging.error("Can't add to database. ({})".format(e))
            logging.error(traceback.print_exc())
            raise (e)


def insert_city_dataframe(session, city_df):
    city_df.columns = ['city_post_code', 'city_name']
    try:
        for index, city_data in city_df.iterrows():
            get_or_create(session, City, city_post_code=city_data['city_post_code'],
                          city_name=address.clean_city(city_data['city_name']))
    except Exception as e:

        logging.error('Rolled back: {}.'.format(e))
        logging.error(city_data)
        logging.error(traceback.print_exc())
        session.rollback()
    else:
        logging.info('Successfully added {} city items to the dataset.'.format(len(city_df)))
        session.commit()


def insert_street_type_dataframe(session, city_df):
    city_df.columns = ['street_type']
    try:
        for index, city_data in city_df.iterrows():
            get_or_create(session, Street_type, street_type=city_data['street_type'])
    except Exception as e:
        logging.error('Rolled back: {}.'.format(e))
        logging.error(city_data)
        logging.error(traceback.print_exc())
        session.rollback()
    else:
        logging.info('Successfully added {} street type items to the dataset.'.format(len(city_df)))
        session.commit()


def insert_common_dataframe(session, common_df):
    common_df.columns = ['poi_name', 'poi_tags', 'poi_url_base', 'poi_code']
    try:
        for index, poi_common_data in common_df.iterrows():
            get_or_create(session, POI_common, **poi_common_data)
    except Exception as e:
        logging.error('Rolled back: {}.'.format(e))
        logging.error(poi_common_data)
        logging.error(traceback.print_exc())
        session.rollback()
    else:
        logging.info('Successfully added {} common items to the dataset.'.format(len(common_df)))
        session.commit()


def search_for_postcode(session, city_name):
    city_col = session.query(City.city_post_code).filter(City.city_name == city_name).all()
    if len(city_col) == 1:
        return city_col
    else:
        logging.info('Cannot determine the post code from city name ({}).'.format(city_name))
        return None


def insert_poi_dataframe(session, poi_df, raw = True):
    if raw is True:
        poi_df.columns = POI_COLS_RAW
    else:
        poi_df.columns = POI_COLS
    poi_df[['poi_postcode']] = poi_df[['poi_postcode']].fillna('0000')
    poi_df[['poi_postcode']] = poi_df[['poi_postcode']].astype(int)
    poi_dict = poi_df.to_dict('records')
    try:
        for poi_data in poi_dict:
            city_col = session.query(City.city_id).filter(City.city_name == poi_data['poi_city']).filter(
                City.city_post_code == poi_data['poi_postcode']).first()
            common_col = session.query(POI_common.pc_id).filter(POI_common.poi_code == poi_data['poi_code']).first()
            poi_data['poi_addr_city'] = city_col
            poi_data['poi_common_id'] = common_col
            if 'poi_name' in poi_data: del poi_data['poi_name']
            if 'poi_code' in poi_data: del poi_data['poi_code']
            if raw is True:
                get_or_create_poi(session, POI_address_raw, **poi_data)
            else:
                get_or_create_poi(session, POI_address, **poi_data)
    except Exception as e:
        logging.error('Rolled back: {}.'.format(e))
        logging.error(poi_data)
        logging.error(traceback.print_exc())
        session.rollback()
        raise (e)
    else:
        try:
            session.commit()
            logging.info('Successfully added {} POI items to the dataset.'.format(len(poi_dict)))
        except Exception as e:
            logging.error('Unsuccessfull commit: {}.'.format(e))
            logging.error(traceback.print_exc())


def insert_type(session, type_data):
    try:
        for i in type_data:
            get_or_create(session, POI_common, **i)
    except Exception as e:
        logging.error('Rolled back: {}.'.format(e))
        logging.error(i)
        logging.error(traceback.print_exc())
        session.rollback()
    else:
        logging.info('Successfully added {} type items to the dataset.'.format(len(type_data)))
        session.commit()


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
        get_or_create(session, POI_address, **kwargs)
    except Exception as e:
        logging.error('Rolled back: {}.'.format(e))
        logging.error(kwargs)
        logging.error(traceback.print_exc())
        session.rollback()
    else:
        logging.debug('Successfully added the item to the dataset.')
        session.commit()
    finally:
        session.close()
