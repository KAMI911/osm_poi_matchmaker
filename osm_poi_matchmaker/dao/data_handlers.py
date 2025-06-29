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
            logging.debug('No additional ref. Search based on address components.')

            required_fields = ['poi_common_id', 'poi_addr_city']
            has_house_data = kwargs.get('poi_addr_street') and kwargs.get('poi_addr_housenumber')
            has_conscription = kwargs.get('poi_conscriptionnumber')

            if all(kwargs.get(field) for field in required_fields) and (has_house_data or has_conscription):
                logging.debug('Fully filled basic data record.')
            else:
                logging.warning('Missing address-related record data: %s', kwargs)

            query = query.filter_by(
                poi_common_id=kwargs.get('poi_common_id'),
                poi_addr_city=kwargs.get('poi_addr_city'),
                poi_addr_street=kwargs.get('poi_addr_street'),
                poi_addr_housenumber=kwargs.get('poi_addr_housenumber'),
                poi_conscriptionnumber=kwargs.get('poi_conscriptionnumber'),
                poi_branch=kwargs.get('poi_branch')
            )

        instance = query.first()
        if instance:
            logging.debug('Already exists: %s', instance)
            return instance

        # No existing instance, create new one
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        logging.debug('Created new instance: %s', instance)
        return instance

    except SQLAlchemyError as e:
        session.rollback()
        logging.error('Database error during get_or_create_poi: %s', e)
        logging.exception('SQLAlchemy exception occurred')
        raise

    except Exception as e:
        session.rollback()
        logging.error('Unexpected error during get_or_create_poi: %s', e)
        logging.exception('General exception occurred')
        raise


def search_poi_patch(session, model, **kwargs):
    if kwargs.get('poi_common_id') == "*":
        kwargs['valami'] = "%"
    if kwargs.get('poi_common_id') == "*":
        kwargs['valami'] = "%"
    if kwargs.get('poi_common_id') == "*":
        kwargs['valami'] = "%"

    instance = session.query(model) \
        .filter_by(poi_common_id=kwargs['poi_common_id'])\
        .filter_by(poi_addr_city=kwargs['poi_addr_city'])\
        .filter_by(poi_addr_street=kwargs['poi_addr_street'])\
        .filter_by(poi_addr_housenumber=kwargs['poi_addr_housenumber'])\
        .filter_by(poi_conscriptionnumber=kwargs['poi_conscriptionnumber'])\
        .filter_by(poi_branch=kwargs['poi_branch'])\
        .first()
    if instance:
        logging.debug('Already added: %s', instance)
        return instance
    else:
        try:
            instance = model(**kwargs)
            session.add(instance)
            session.commit()
            return instance
        except Exception as e:
            logging.error('Cannot add to the database. (%s)', e)
            logging.exception('Exception occurred')
            raise e


def get_or_create_cache(session, model, **kwargs):
    """
    Retrieve an existing cache entry from the database or create a new one if it does not exist.

    This function looks for a model instance based on the given `osm_id` and `osm_object_type`.
    If such an instance exists, it is returned. Otherwise, a new instance is created,
    added to the session, and committed to the database.

    Args:
        session (sqlalchemy.orm.Session): The SQLAlchemy session used for querying and committing.
        model (Base): The SQLAlchemy model class representing the cache table.
        **kwargs: Keyword arguments used for both querying and creating the model.
            Required keys include:
                - osm_id (int or str): The OpenStreetMap object ID.
                - osm_object_type (str): The type of the OSM object (e.g. 'node', 'way', 'relation').

    Returns:
        model: The retrieved or newly created model instance.

    Raises:
        Exception: If database operations fail.
    """
    osm_id = kwargs.get('osm_id')
    osm_object_type = kwargs.get('osm_object_type')

    if osm_id is not None and osm_object_type:
        try:
            instance = session.query(model).filter_by(
                osm_id=osm_id,
                osm_object_type=osm_object_type
            ).first()

            if instance:
                logging.debug('Instance already exists: %s', instance)
                return instance

            instance = model(**kwargs)
            session.add(instance)
            session.commit()
            logging.debug('New instance created: %s', instance)
            return instance

        except SQLAlchemyError as e:
            session.rollback()
            logging.error('Failed to add to the database: %s', e)
            logging.exception('SQLAlchemy exception details:')
            raise

        except Exception as e:
            session.rollback()
            logging.error('Unexpected error during cache insert: %s', e)
            logging.exception('General exception details:')
            raise

    else:
        logging.warning('Missing required keys: osm_id or osm_object_type')
        return None


def get_or_create_common(session, model, **kwargs):
    """
    Retrieve an existing database record by `poi_code` or create a new one if it does not exist.

    This function attempts to find an existing instance of the given model based on the `poi_code` field.
    If a matching record is found, it is returned. Otherwise, a new instance is created using the provided
    keyword arguments, added to the database, and committed.

    Args:
        session (sqlalchemy.orm.Session): The SQLAlchemy session used for querying and committing.
        model (Base): The SQLAlchemy model class representing the target table.
        **kwargs: Arbitrary keyword arguments used for both querying and creating the model.
                  Must include the key `poi_code`.

    Returns:
        model: The retrieved or newly created model instance.

    Raises:
        SQLAlchemyError: If a database error occurs during insertion.
        Exception: If any other unexpected error occurs.

    Notes:
        If `poi_code` is missing or empty, the function logs a warning and returns None.
    """
    poi_code = kwargs.get('poi_code')

    if poi_code:
        instance = session.query(model).filter_by(poi_code=poi_code).first()
        if instance:
            logging.debug('Instance already exists: %s', instance)
            return instance
        else:
            try:
                instance = model(**kwargs)
                session.add(instance)
                session.commit()
                logging.debug('New instance created: %s', instance)
                return instance
            except SQLAlchemyError as e:
                session.rollback()
                logging.error('Database error while adding instance with poi_code=%s: %s', poi_code, e)
                logging.exception('SQLAlchemy exception occurred:')
                raise
            except Exception as e:
                session.rollback()
                logging.error('Unexpected error while adding instance with poi_code=%s: %s', poi_code, e)
                logging.exception('General exception occurred:')
                raise
    else:
        logging.warning('Missing or empty poi_code in input: %s', kwargs)
        return None


def insert_city_dataframe(session, city_df: pd.DataFrame):
    """
    Inserts city data from a DataFrame into the database if the counts differ.

    This function compares the number of city records in the database with the number of
    rows in the provided DataFrame. If the counts are the same, it skips the insertion.
    Otherwise, it iterates through the DataFrame, cleans the city names, and uses `get_or_create`
    to insert or fetch existing city records.

    Args:
        session (sqlalchemy.orm.Session): The SQLAlchemy session to use for DB operations.
        city_df (pandas.DataFrame): DataFrame containing city data with columns for postal code and city name.

    Raises:
        Exception: Any exceptions raised during the database insertion are logged and re-raised.
    """
    db_count = count(session, City)
    df_count = len(city_df)
    logging.debug(f'City DB count = {db_count}, DataFrame count = {df_count}')

    if db_count == df_count:
        logging.debug('City DataFrame count matches DB row count; skipping processing.')
        session.close()
        return

    # Ensure consistent column names
    city_df.columns = ['city_post_code', 'city_name']

    try:
        for _, city_data in city_df.iterrows():
            cleaned_city_name = address.clean_city(city_data['city_name'])
            get_or_create(session, City,
                          city_post_code=city_data['city_post_code'],
                          city_name=cleaned_city_name)
    except Exception as e:
        logging.error(f'Error occurred, rolling back transaction: {e}')
        logging.error(f'Failed city data: {city_data}')
        logging.exception('Exception traceback:')
        session.rollback()
        raise
    else:
        session.commit()
        logging.info(f'Successfully added {df_count} city items to the dataset.')
    finally:
        session.close()


def query_city_name_external(session, city_name: str, zip_code: str,
                             similarity_threshold: float = 0.7,
                             levenshtein_threshold: int = 3) -> str | None:
    """
    Search for a city name in the database with multiple matching strategies.

    The function tries the following search strategies in order:
    1. Exact match on city_name and zip_code.
    2. Match by soundex, similarity, levenshtein distance, and zip_code.
    3. Match by similarity, levenshtein distance, and zip_code.
    4. Match by similarity and levenshtein distance only.
    5. Match by zip_code only as a fallback.

    Args:
        session (sqlalchemy.orm.Session): Database session for querying.
        city_name (str): The city name to search for.
        zip_code (str): The postal code associated with the city.
        similarity_threshold (float): Minimum similarity score (0-1) to consider a match.
        levenshtein_threshold (int): Maximum Levenshtein distance to consider a match.

    Returns:
        str | None: The matched city name from the database or None if no match is found.
    """
    try:
        logging.debug('Exact name and postal code based search...')
        result = session.query(City)\
            .filter(City.city_name == city_name)\
            .filter(City.city_post_code == zip_code)\
            .first()
        if result:
            logging.debug(f'Exact match found: {result.city_name}, postcode: {zip_code}')
            return result.city_name

        logging.debug('Soundex + similarity + Levenshtein + postal code based search...')
        result = session.query(City)\
            .filter(func.soundex(City.city_name) == func.soundex(city_name))\
            .filter(func.similarity(City.city_name, city_name) > similarity_threshold)\
            .filter(func.levenshtein(City.city_name, city_name) < levenshtein_threshold)\
            .filter(City.city_post_code == zip_code)\
            .order_by(func.similarity(City.city_name, city_name).desc())\
            .first()
        if result:
            logging.debug(f'Soundex-similarity match found: {result.city_name}, postcode: {zip_code}')
            return result.city_name

        logging.debug('Similarity + Levenshtein + postal code based search...')
        result = session.query(City)\
            .filter(func.similarity(City.city_name, city_name) > similarity_threshold)\
            .filter(func.levenshtein(City.city_name, city_name) < levenshtein_threshold)\
            .filter(City.city_post_code == zip_code)\
            .order_by(func.similarity(City.city_name, city_name).desc())\
            .first()
        if result:
            logging.debug(f'Similarity-Levenshtein match found: {result.city_name}, postcode: {zip_code}')
            return result.city_name

        logging.debug('Similarity + Levenshtein based search (without postal code)...')
        result = session.query(City)\
            .filter(func.similarity(City.city_name, city_name) > similarity_threshold)\
            .filter(func.levenshtein(City.city_name, city_name) < levenshtein_threshold)\
            .order_by(func.similarity(City.city_name, city_name).desc())\
            .first()
        if result:
            logging.debug(f'Similarity-Levenshtein match found: {result.city_name}')
            return result.city_name

        logging.debug('Postal code only based search as fallback...')
        if zip_code:
            result = session.query(City)\
                .filter(City.city_post_code == zip_code)\
                .first()
            if result:
                logging.debug(f'Fallback found city {result.city_name} for postcode: {zip_code}')
                return result.city_name

        logging.debug(f'No matching city found for {city_name} with postcode {zip_code}')
        return None

    except Exception as e:
        logging.error(f'Error during city name query: {e}')
        logging.exception('Exception traceback:')
        return None
    finally:
        session.commit()


def insert_street_type_dataframe(session: Session, street_df: pd.DataFrame):
    """
    Inserts unique street types from a DataFrame into the Street_type table.

    The function compares the number of existing records in the database with
    the number of rows in the DataFrame. If they match, it skips the insertion.

    Args:
        session (sqlalchemy.orm.Session): The database session to use.
        street_df (pandas.DataFrame): DataFrame containing street type data.

    Returns:
        None
    """
    db_count = count(session, Street_type)
    df_count = len(street_df)
    logging.debug(f'street db_count={db_count} patch_count={df_count}')

    if db_count == df_count:
        logging.debug('Street dataframe count matches DB row count; skipping data processing.')
        session.close()
        return

    # Rename columns to expected names for uniformity
    street_df.columns = ['street_type']

    try:
        for _, street_data in street_df.iterrows():
            get_or_create(session, Street_type, street_type=street_data['street_type'])
    except Exception as e:
        logging.error(f'Rolled back due to error: {e}')
        logging.error(f'Offending data row: {street_data}')
        logging.exception('Exception occurred during street type insertion.')
        session.rollback()
    else:
        logging.info(f'Successfully added {len(street_df)} street type items to the dataset.')
        session.commit()
    finally:
        session.close()


def insert_patch_data_dataframe(session: Session, patch_df: pd.DataFrame):
    db_count = count(session, POI_patch)
    df_count = len(patch_df)
    logging.debug('poi patch db_count={} patch_count={}'.format(db_count, df_count))
    if db_count == df_count:
        logging.debug('poi patch dataframe count same as db row count, skipping processing data')
        session.close()
        return

    patch_df.columns = ['poi_code', 'orig_postcode', 'orig_city', 'orig_street', 'orig_housenumber',
                        'orig_conscriptionnumber', 'orig_name', 'new_postcode', 'new_city', 'new_street',
                        'new_housenumber', 'new_conscriptionnumber', 'new_name']
    try:
        for index, patch_data in patch_df.iterrows():
            get_or_create(session, POI_patch, poi_code=str(patch_data['poi_code']),
                          orig_postcode=str(patch_data['orig_postcode']),
                          orig_city=str(patch_data['orig_city']), orig_street=str(patch_data['orig_street']),
                          orig_housenumber=str(patch_data['orig_housenumber']),
                          orig_conscriptionnumber=str(patch_data['orig_conscriptionnumber']),
                          orig_name=str(patch_data['orig_name']), new_postcode=str(patch_data['new_postcode']),
                          new_city=str(patch_data['new_city']),
                          new_street=str(patch_data['new_street']), new_housenumber=str(patch_data['new_housenumber']),
                          new_conscriptionnumber=str(patch_data['new_conscriptionnumber']),
                          new_name=str(patch_data['new_name']))
    except Exception as e:
        logging.error(f'Rolled back: {e}.')
        logging.error(patch_data)
        logging.exception('Exception occurred')

        session.rollback()
    else:
        logging.info('Successfully added %s patch items to the dataset.', len(patch_df))
        session.commit()
    finally:
        session.close()


def insert_country_data_dataframe(session: Session, country_df: pd.DataFrame) -> None:
    """
    Inserts country data from a DataFrame into the Country table.

    The function compares the number of records in the database with the
    number of rows in the DataFrame. If they match, the insertion is skipped.

    Args:
        session (sqlalchemy.orm.Session): The active database session.
        country_df (pandas.DataFrame): DataFrame containing country data with columns:
            ['country_code', 'continent_code', 'country_name', 'country_iso3',
             'country_number', 'country_full_name']

    Returns:
        None
    """
    db_count = count(session, Country)
    df_count = len(country_df)  # Includes all rows in the DataFrame
    logging.debug(f'country db_count={db_count} patch_count={df_count}')

    if db_count == df_count:
        logging.debug('Country dataframe count matches DB row count; skipping data processing.')
        session.close()
        return

    # Ensure DataFrame columns are named as expected
    country_df.columns = [
        'country_code', 'continent_code', 'country_name', 'country_iso3',
        'country_number', 'country_full_name'
    ]

    try:
        for _, country_data in country_df.iterrows():
            get_or_create(
                session, Country,
                country_code=country_data['country_code'],
                continent_code=country_data['continent_code'],
                country_name=country_data['country_name'],
                country_iso3=country_data['country_iso3'],
                country_number=country_data['country_number'],
                country_full_name=country_data['country_full_name']
            )
    except Exception as e:
        logging.error(f'Rolled back due to error: {e}')
        logging.error(f'Offending data row: {country_data}')
        logging.exception('Exception occurred during country data insertion.')
        session.rollback()
    else:
        logging.info(f'Successfully added {len(country_df)} country items to the dataset.')
        session.commit()
    finally:
        session.close()


def insert_common_dataframe(session, common_df):
    common_df.columns = ['poi_common_name', 'poi_tags', 'poi_url_base', 'poi_code']
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


def search_for_postcode(session: Session, city_name: str) -> str | None:
    """
    Searches for the postcode of a city by its name.

    Args:
        session (Session): SQLAlchemy session object.
        city_name (str): The name of the city to search for.

    Returns:
        Optional[str]: The postcode as a string if found; otherwise None.
    """
    try:
        # Query the city_post_code column for the given city_name
        city_codes = session.query(City.city_post_code).filter(City.city_name == city_name).all()

        if not city_codes:
            logging.info('No post code found for city name: %s', city_name)
            return None

        if len(city_codes) > 1:
            logging.info('Multiple post codes found for city name: %s, unable to determine uniquely.', city_name)
            return None

        if len(city_codes) == 1:
            postcode = city_codes[0][0]  # city_codes is a list of tuples
            if postcode not in (None, 0, '0', ''):
                return postcode
            else:
                return None
        else:
            logging.info('Cannot determine the post code from city name (%s).', city_name)
            return None
    except Exception as e:
        logging.error('Error: %s.', e)
        logging.exception('Exception occurred')
        return None
    finally:
        session.commit()


def insert_poi_dataframe(session: Session, poi_df: pd.DataFrame, raw: bool = True) -> None:
    """
    Inserts POI data from a DataFrame into the appropriate POI_address table.

    Args:
        session (Session): The SQLAlchemy database session.
        poi_df (pd.DataFrame): DataFrame containing POI data.
        raw (bool): If True, inserts into POI_address_raw; else into POI_address.

    Returns:
        None
    """
    if raw:
        poi_df.columns = POI_COLS_RAW

    poi_df['poi_postcode'] = poi_df['poi_postcode'].astype(object).where(poi_df['poi_postcode'].notna(), None)

    poi_dict = poi_df.to_dict('records')

    df_count = len(poi_df)
    model = POI_address_raw if raw else POI_address
    db_count = count(session, model)

    logging.debug(f'poi address raw={raw} db_count={db_count} patch_count={df_count}')

    if db_count == df_count:
        logging.debug(f'poi address raw={raw} dataframe count same as db row count, skipping processing data')
        session.close()
        return

    try:
        for poi_data in poi_dict:
            city_col = session.query(City.city_id)\
                .filter(City.city_name == poi_data.get('poi_city'))\
                .filter(City.city_post_code == poi_data.get('poi_postcode'))\
                .first()
            common_col = session.query(POI_common.pc_id)\
                .filter(POI_common.poi_code == poi_data.get('poi_code'))\
                .first()

            if city_col is not None:
                poi_data['poi_addr_city'] = city_col[0]
            if common_col is not None:
                poi_data['poi_common_id'] = common_col[0]

            logging.debug(f"POI Name is {poi_data.get('poi_name')}")

            poi_data.pop('poi_code', None)

            get_or_create_poi(session, model, **poi_data)

    except Exception as e:
        logging.exception(f'Exception occurred: {e} rolled back: {traceback.format_exc()}')
        session.rollback()
        raise

    else:
        try:
            session.commit()
            logging.info(f'Successfully added {len(poi_dict)} POI items to the dataset.')
        except Exception as e:
            logging.exception(f'Exception occurred during commit: {e} rolled back: {traceback.format_exc()}')
            session.rollback()
            raise
    finally:
        session.close()


def insert_type(session, type_data):
    try:
        for i in type_data:
            get_or_create_common(session, POI_common, **i)

        logging.info('Successfully added %s type items to the dataset.', len(type_data))
        session.commit()
    except Exception as e:
        logging.error('Rolled back: %s.', e)
        logging.error(i)
        logging.exception('Exception occurred')

        session.rollback()
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
        # if 'poi_name' in kwargs: del kwargs['poi_name']
        if 'poi_code' in kwargs: del kwargs['poi_code']
        get_or_create_poi(session, POI_address, **kwargs)
    except Exception as e:
        logging.error('Rolled back: %s.', e)
        logging.error(kwargs)
        logging.exception('Exception occurred')

        session.rollback()
    else:
        try:
            session.commit()
            logging.debug('Successfully added the item to the dataset.')
        except Exception as e:
            logging.exception('Exception occurred: {} unsuccessfully commit: {}'.format(e, traceback.format_exc()))
            session.rollback()
    finally:
        session.close()
