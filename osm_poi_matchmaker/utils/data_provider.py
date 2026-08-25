# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    from osm_poi_matchmaker.dao.data_handlers import insert_poi_dataframe
    from osm_poi_matchmaker.libs.address import clean_city, \
        clean_javascript_variable, clean_opening_hours_2, clean_phone
    from osm_poi_matchmaker.libs.poi_dataset import POIDatasetRaw
    from osm_poi_matchmaker.utils.enums import FileType
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

POI_DATA = ''


class DataProvider:
    """Base class every hu_* data provider subclasses.

    A concrete provider overrides contains() (to set self.link, self.tags and
    self.filetype/self.filename), types() (to describe the OSM tag sets it can
    produce) and process() (to fetch the data and call self.data.add() per POI).
    The standard harvest flow (see import_poi_data_module.py) is:

        work = mo(session, download_cache)   # __init__ calls contains()
        insert_type(session, work.types())
        work.process()
        stats = work.export_list()
    """

    def __init__(self, session, download_cache, filetype=FileType.json, verify_link=True):
        """Set up the provider and immediately call contains() to configure it.

        Args:
            session: SQLAlchemy session used to write POI type/common data.
            download_cache (str): Directory where downloaded source files are cached.
            filetype (FileType): Default file type for self.filename (providers usually
                override this again in contains()).
            verify_link (bool): Whether to verify TLS certificates when downloading.
        """
        self.session = session
        self.download_cache = download_cache
        self.filename = '{}.{}'.format(self.__class__.__name__, filetype)
        self.filetype = None
        self.verify_link = verify_link
        self.link = None
        self.POI_COMMON_TAGS = None
        self.headers = None
        self.post = None
        self.tags = None
        self.__types = None
        self.contains()
        self.data = POIDatasetRaw()

    def contains(self):
        """Configure the provider: set self.link (source URL), self.tags (common OSM
        tags shared by every POI type below) and self.filetype/self.filename.
        Overridden by every concrete provider; this base implementation is a no-op
        placeholder."""
        self.POI_COMMON_TAGS = ""
        self.link = ''

    def types(self):
        """Return the list of POI type definitions this provider can produce.

        Each entry is a dict with keys like 'poi_code', 'poi_common_name', 'poi_type',
        'poi_tags', 'poi_url_base', 'poi_search_name' and the osm_search_distance_*
        thresholds used by the matcher. Overridden by every concrete provider.

        Returns:
            list[dict]: POI type definitions, or an empty list from this base class.
        """
        self.__types = []
        return self.__types

    def process(self):
        """Fetch the source data and call self.data.add() for each POI found.
        Overridden by every concrete provider; this base implementation is a no-op."""
        pass

    def export_list(self) -> dict:
        """Write everything harvested by process() (self.data) to the database.

        Returns:
            dict: Harvest/insert stats with keys 'harvested', 'harvest_errors',
            'db_inserted', 'db_errors' (all 0 if the result set was empty).
        """
        stats = {'harvested': 0, 'harvest_errors': 0, 'db_inserted': 0, 'db_errors': 0}
        if self.data is not None:
            stats.update(self.data.stats())
        if self.data is None or self.data.length() < 1:
            logging.warning('Resultset is empty. Skipping ...')
        else:
            insert_stats = insert_poi_dataframe(self.session, self.data.process())
            stats['db_inserted'] = insert_stats.get('inserted', 0)
            stats['db_errors'] = insert_stats.get('errors', 0)
        return stats
