#!/usr/bin/python
# -*- coding: utf-8 -*-
import osm_poi_matchmaker.libs.geo

try:
    import requests
    import sqlalchemy
    import sqlalchemy.orm
    import pandas as pd
    import re
    import os
    import json
    import logging, logging.config
    import hashlib
    from bs4 import BeautifulSoup
    from osm_poi_matchmaker.libs import address, geo
    from osm_poi_matchmaker.dao.data_structure import Base, City, POI_address, POI_common
    from osm_poi_matchmaker.libs.file_output import save_csv_file
    from geoalchemy2 import WKTElement
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


__program__ = 'create_db'
__version__ = '0.3.0'


output_folder = '.'

DOWNLOAD_CACHE = '../cache_url'
PATTERN_SPAR_REF = re.compile('\((.*?)\)')
PROJ = 4326


def init_log():
    logging.config.fileConfig('log.conf')


def download_soup(link):
    page = requests.get(link, verify=False)
    return BeautifulSoup(page.content, 'html.parser') if page.status_code == 200 else None


def save_downloaded_soup(link, file):
    soup = download_soup(link)
    with open(file, mode="w", encoding="utf8") as code:
        code.write(str(soup.prettify()))
    return soup


def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        return instance

def insert_type(session, type_data):
    try:
        for i in type_data:
            get_or_create(session, POI_common, poi_name=i['poi_name'], poi_tags=i['poi_tags'], poi_url_base=i['poi_url_base'], poi_code=i['poi_code'])
    except Exception as e:
        print(e)


def insert(session, **kwargs):
    try:
        city_col = session.query(City.city_id).filter(City.city_name == kwargs['poi_city']).filter(
            City.city_post_code == kwargs['poi_postcode']).first()
        common_col = session.query(POI_common.pc_id).filter(POI_common.poi_code == kwargs['poi_code']).first()
        kwargs['poi_addr_city'] = city_col
        kwargs['poi_common_id'] = common_col
        kwargs['poi_hash'] = hashlib.sha512('{}{}{}{}{}{}'.format(kwargs['poi_code'], kwargs['poi_postcode'], kwargs['poi_city'], kwargs['poi_addr_street'], kwargs['poi_addr_housenumber'], kwargs['poi_conscriptionnumber'] ).lower().replace(' ','').encode('utf-8')).hexdigest()
        if 'poi_name' in kwargs: del kwargs['poi_name']
        if 'poi_code' in kwargs: del kwargs['poi_code']
        get_or_create(session, POI_address, **kwargs )
        session.commit()
    except Exception as e:
        print(e)
    finally:
        session.close()

class POI_Base:
    """Represents the full database.

    :param db_conection: Either a sqlalchemy database url or a filename to be used with sqlite.

    """

    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.db_filename = None
        if '://' not in db_connection:
            self.db_connection = 'sqlite:///%s' % self.db_connection
        if self.db_connection.startswith('sqlite'):
            self.db_filename = self.db_connection
        self.engine = sqlalchemy.create_engine(self.db_connection, echo=False)
        Session = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.session = Session()
        Base.metadata.create_all(self.engine)


    def add_poi_types(self, data):
        insert_type(self.session, data)


    def add_city(self, link_base):
        xl = pd.ExcelFile(link_base)
        df = xl.parse("Települések")
        for index, city_data in df.iterrows():
            try:
                get_or_create(self.session, City, city_post_code=city_data['IRSZ'], city_name=address.clean_city(city_data['Település']))
                self.session.commit()
            except Exception as e:
                print(e)
            finally:
                self.session.close()
        big_cities = [['Budapest', 'Bp.u.'],
                      ['Miskolc', 'Miskolc u.'],
                      ['Debrecen', 'Debrecen u.'],
                      ['Szeged', 'Szeged u.'],
                      ['Pécs', 'Pécs u.'],
                      ['Győr', 'Győr u.']
                      ]
        for city, sheet in big_cities:
            df = xl.parse(sheet)
            for index, city_data in df.iterrows():
                try:
                    get_or_create(self.session, City, city_post_code=city_data[0], city_name=city)
                    self.session.commit()
                except Exception as e:
                    print(e)
                finally:
                    self.session.close()


    def add_tesco(self, link_base):
        soup = save_downloaded_soup('{}'.format(link_base), os.path.join(DOWNLOAD_CACHE, 'tesco.html'))
        data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            table = soup.find('table', attrs={'class': 'tescoce-table'})
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                link = cols[0].find('a').get('href') if cols[0].find('a') != None else []
                cols = [element.text.strip() for element in cols]
                cols[0] = cols[0].split('\n')[0]
                del cols[-1]
                del cols[-1]
                cols.append(link)
                data.append(cols)
            for poi_data in data:
                insert_row = {}
                # street, housenumobject does not support indexingber = address.extract_street_housenumber(poi_data[3])
                street, housenumber, conscriptionnumber = address.extract_street_housenumber_better(poi_data[3])
                tesco_replace = re.compile('(expressz{0,1})', re.IGNORECASE)
                poi_data[0] = tesco_replace.sub('Expressz', poi_data[0])
                if 'xpres' in poi_data[0]:
                    name = 'Tesco Expressz'
                    code = 'hutescoexp'
                elif 'xtra' in poi_data[0]:
                    name = 'Tesco Extra'
                    code = 'hutescoext'
                else:
                    name = 'Tesco'
                    code = 'hutescosup'
                poi_data[0] = poi_data[0].replace('TESCO', 'Tesco')
                poi_data[0] = poi_data[0].replace('Bp.', 'Budapest')

                insert(self.session, poi_code = code, poi_city = address.clean_city(poi_data[2].split(',')[0]), poi_name = name, poi_postcode = poi_data[1].strip(), poi_branch = poi_data[0], poi_website = poi_data[4], original = poi_data[3], poi_addr_street = street, poi_addr_housenumber = housenumber, poi_conscriptionnumber = conscriptionnumber)


    def add_aldi(self, link_base):
        soup = save_downloaded_soup('{}'.format(link_base), os.path.join(DOWNLOAD_CACHE, 'aldi.html'))
        data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            table = soup.find('table', attrs={'class': 'contenttable is-header-top'})
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [element.text.strip() for element in cols]
                data.append(cols)
            for poi_data in data:
                street, housenumber, conscriptionnumber = address.extract_street_housenumber_better(poi_data[2])
                name = 'Aldi'
                code = 'hualdisup'
                insert(self.session, poi_code = code, poi_city = address.clean_city(poi_data[1]), poi_name = name, poi_postcode =  poi_data[0].strip(), poi_branch = None, poi_website = None, original = poi_data[2], poi_addr_street = street, poi_addr_housenumber = housenumber, poi_conscriptionnumber = conscriptionnumber)


    def add_cba(self, link_base):
        soup = save_downloaded_soup('{}'.format(link_base), os.path.join(DOWNLOAD_CACHE, 'cba.html'))
        data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            pattern = re.compile('^\s*var\s*boltok_nyers.*')
            script = soup.find('script', text=pattern)
            m = pattern.match(script.get_text())
            data = m.group(0)
            data = address.clean_javascript_variable(data, 'boltok_nyers')
            text = json.loads(data)
            # for l in text:
            # print ('postcode: {postcode}; city: {city}; address: {address}; alt_name: {alt_name}'.format(postcode=l['A_IRSZ'], city=l['A_VAROS'], address=l['A_CIM'], alt_name=l['P_NAME']))

            for poi_data in text:
                street, housenumber, conscriptionnumber = address.extract_street_housenumber_better(poi_data['A_CIM'])
                city = address.clean_city(poi_data['A_VAROS'])
                postcode = poi_data['A_IRSZ'].strip()
                branch = poi_data['P_NAME'].strip()
                name = 'Príma' if 'Príma' in branch else 'CBA'
                code = 'huprimacon'  if 'Príma' in branch else 'hucbacon'
                insert(self.session, poi_code = code, poi_city = city, poi_name = name, poi_postcode = postcode, poi_branch = branch, poi_website = None, original = poi_data['A_CIM'], poi_addr_street = street, poi_addr_housenumber = housenumber, poi_conscriptionnumber = conscriptionnumber, geom_hint = osm_poi_matchmaker.libs.geo.geom_point(poi_data['PS_GPS_COORDS_LAT'], poi_data['PS_GPS_COORDS_LNG'], PROJ) )


    def add_rossmann(self, link_base):
        soup = save_downloaded_soup('{}'.format(link_base), os.path.join(DOWNLOAD_CACHE, 'rossmann.html'))
        data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            pattern = re.compile('^\s*var\s*places.*')
            script = soup.find('script', text=pattern)
            m = pattern.match(script.get_text())
            data = m.group(0)
            data = address.clean_javascript_variable(data, 'places')
            text = json.loads(data)
            # for l in text:
            # print ('postcode: {postcode}; city: {city}; address: {address}; alt_name: {alt_name}'.format(postcode=l['A_IRSZ'], city=l['A_VAROS'], address=l['A_CIM'], alt_name=l['P_NAME']))

            for poi_data in text:
                street, housenumber, conscriptionnumber = address.extract_street_housenumber_better(
                    poi_data['addresses'][0]['address'])
                name = 'Rossmann'
                code = 'hurossmche'
                insert(self.session, poi_code = code, poi_city = address.clean_city(poi_data['city']), poi_name = name, poi_postcode = poi_data['addresses'][0]['zip'].strip(), poi_branch = None, poi_website = None, geom_hint = osm_poi_matchmaker.libs.geo.geom_point(poi_data['addresses'][0]['position'][1], poi_data['addresses'][0]['position'][0], PROJ), original = poi_data['addresses'][0]['address'], poi_addr_street = street, poi_addr_housenumber = housenumber, poi_conscriptionnumber = conscriptionnumber)


    def add_spar(self, link_base):
        soup = save_downloaded_soup('{}'.format(link_base), os.path.join(DOWNLOAD_CACHE, 'spar.json'))
        data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text:
                street, housenumber, conscriptionnumber = address.extract_street_housenumber_better(poi_data['address'])
                if 'xpres' in poi_data['name']:
                    name = 'Spar Expressz'
                    code = 'husparexp'
                elif 'INTER' in poi_data['name']:
                    name = 'Interspar'
                    code = 'husparint'
                elif 'market' in poi_data['name']:
                    name = 'Spar'
                    code = 'husparsup'
                else:
                    name = 'Spar'
                    code = 'husparsup'
                poi_data['name'] = poi_data['name'].replace('INTERSPAR', 'Interspar')
                poi_data['name'] = poi_data['name'].replace('SPAR', 'Spar')
                ref_match = PATTERN_SPAR_REF.search(poi_data['name'])
                ref = ref_match.group(1).strip() if ref_match is not None else None
                insert(self.session, poi_code = code, poi_city = address.clean_city(poi_data['city']), poi_name = name, poi_postcode = poi_data['zipCode'].strip(), poi_branch = poi_data['name'].split('(')[0].strip(), poi_website = poi_data['pageUrl'].strip(), geom_hint = osm_poi_matchmaker.libs.geo.geom_point(poi_data['latitude'], poi_data['longitude'], PROJ), original = poi_data['address'], poi_addr_street = street, poi_addr_housenumber = housenumber, poi_conscriptionnumber = conscriptionnumber, poi_ref = ref)


    def add_kh_bank(self, link_base, name = 'K&H bank'):
        if link_base:
            with open(link_base, 'r') as f:
                text = json.load(f)
                for poi_data in text['results']:
                    first_element = next(iter(poi_data))
                    if name == 'K&H bank':
                        code = 'hukhbank'
                    else:
                        code = 'hukhatm'
                    postcode, city, street, housenumber, conscriptionnumber = address.extract_all_address(poi_data[first_element]['address'])
                    insert(self.session, poi_code = code, poi_city=city, poi_name=name,
                           poi_postcode=postcode, poi_branch=None,
                           poi_website=None, geom_hint = osm_poi_matchmaker.libs.geo.geom_point(poi_data[first_element]['latitude'], poi_data[first_element]['longitude'], PROJ),
                           original=poi_data[first_element]['address'], poi_addr_street=street,
                           poi_addr_housenumber=housenumber, poi_conscriptionnumber=conscriptionnumber, poi_ref=None)

    def add_cib_bank(self, link_base):
        return True


    def add_benu(self, link_base):
        soup = save_downloaded_soup('{}'.format(link_base), os.path.join(DOWNLOAD_CACHE, 'benu.json'))
        data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text:
                street, housenumber, conscriptionnumber = address.extract_street_housenumber_better(poi_data['street'])
                if 'BENU Gyógyszertár' not in poi_data['title']:
                    name = poi_data['title'].strip()
                    branch = None
                else:
                    name = 'Benu gyógyszertár'
                    branch = poi_data['title'].strip()
                code = 'hubenupha'
                website = poi_data['description'].strip() if poi_data['description'] is not None else None
                insert(self.session, poi_code = code, poi_city = address.clean_city(poi_data['city']), poi_name = name, poi_postcode = poi_data['postal_code'].strip(), poi_branch = branch, poi_website = website, geom_hint = osm_poi_matchmaker.libs.geo.geom_point(poi_data['lat'], poi_data['lng'], PROJ), original = poi_data['street'], poi_addr_street = street, poi_addr_housenumber = housenumber, poi_conscriptionnumber = conscriptionnumber, poi_ref = None)

    def add_posta(self, link_base, filename):
        soup = save_downloaded_soup('{}'.format(link_base), os.path.join(DOWNLOAD_CACHE, filename))
        data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text['items']:
                if poi_data['type'] == 'posta':
                    if 'mobilposta' in poi_data['name']:
                        name = 'Mobilposta'
                        code = 'hupostamp'
                    else:
                        name = 'Posta'
                        code = 'hupostapo'
                elif poi_data['type'] == 'csekkautomata':
                    name = 'Posta csekkautomata'
                    code = 'hupostacse'
                elif poi_data['type'] == 'postaautomata':
                    name = 'Posta csomagautomata'
                    code = 'hupostacso'
                elif poi_data['type'] == 'postapoint':
                    name = 'PostaPont'
                    code = 'hupostapp'
                else:
                    logging.error('Non existing Posta type.')
                postcode = poi_data['zipCode'].strip()
                street, housenumber, conscriptionnumber = address.extract_street_housenumber_better(poi_data['address'])
                city = address.clean_city(poi_data['city'])
                branch = poi_data['name']
                website = None
                insert(self.session, poi_code = code, poi_city = city, poi_name = name, poi_postcode = postcode, poi_branch = branch, poi_website = website, geom_hint = osm_poi_matchmaker.libs.geo.geom_point(poi_data['lat'], poi_data['lng'], PROJ)
, original = poi_data['address'], poi_addr_street = street, poi_addr_housenumber = housenumber, poi_conscriptionnumber = conscriptionnumber, poi_ref = None)


    def add_foxpost(self, link_base, filename):
        soup = save_downloaded_soup('{}'.format(link_base), os.path.join(DOWNLOAD_CACHE, filename))
        data = []
        if soup != None:
            text = json.loads(soup.get_text())
            for poi_data in text:
                name = 'Foxpost'
                code = 'hufoxpocso'
                postcode = poi_data['zip'].strip()
                street, housenumber, conscriptionnumber = address.extract_street_housenumber_better(poi_data['street'])
                city = address.clean_city(poi_data['city'])
                branch = poi_data['name']
                website = None
                mo = poi_data['open']['hetfo'].strip() if poi_data['open']['hetfo'] is not None else None
                th = poi_data['open']['kedd'].strip() if poi_data['open']['kedd'] is not None else None
                we = poi_data['open']['szerda'].strip() if poi_data['open']['szerda'] is not None else None
                tu = poi_data['open']['csutortok'].strip() if poi_data['open']['csutortok'] is not None else None
                fr = poi_data['open']['pentek'].strip() if poi_data['open']['pentek'] is not None else None
                sa = poi_data['open']['szombat'].strip() if poi_data['open']['szombat'] is not None else None
                su = poi_data['open']['vasarnap'].strip() if poi_data['open']['vasarnap'] is not None else None
                insert(self.session, poi_code = code, poi_city = city, poi_name = name, poi_postcode = postcode, poi_branch = branch, poi_website = website, original = poi_data['address'], poi_addr_street = street, poi_addr_housenumber = housenumber, poi_conscriptionnumber = conscriptionnumber, poi_ref = None, geom_hint = osm_poi_matchmaker.libs.geo.geom_point(poi_data['geolat'], poi_data['geolng'], PROJ), poi_opening_hours_mo = mo, poi_opening_hours_tu = tu, poi_opening_hours_we = we, poi_opening_hours_th = th, poi_opening_hours_fr = fr, poi_opening_hours_sa = sa, poi_opening_hours_su = su)


    def add_avia(self, link_base):
        soup = save_downloaded_soup('{}'.format(link_base), os.path.join(DOWNLOAD_CACHE, 'avia.html'))
        data = []
        if soup != None:
            # parse the html using beautiful soap and store in variable `soup`
            #pattern = re.compile('var\s*markers\s*=\s*.*', re.MULTILINE)
            pattern = re.compile('var\s*markers\s*=\s*((.*\n)*\]\;)', re.MULTILINE)
            script = soup.find('script', text=pattern)
            m = pattern.search(script.get_text())
            data = m.group(0)
            data = data.replace("'", '"')
            data = address.clean_javascript_variable(data, 'markers')
            text = json.loads(data)
            for poi_data in text:
                postcode, city, street, housenumber, conscriptionnumber = address.extract_all_address(poi_data['cim'])
                name = 'Avia'
                code = 'huaviafu'
                branch = ''
                ref = poi_data['kutid'] if poi_data['kutid'] is not None and poi_data['kutid'] != '' else None
                if (poi_data['lat'] is not None and poi_data['lat'] != '') and (poi_data['lng'] is not None and poi_data['lng'] != ''):
                    geom_hint = osm_poi_matchmaker.libs.geo.geom_point(poi_data['lat'], poi_data['lng'], PROJ)
                else:
                    geom_hint = None
                insert(self.session, poi_code = code, poi_city = city, poi_name = name, poi_postcode = postcode, poi_branch = branch, poi_website = None, original = poi_data['cim'], poi_addr_street = street, poi_addr_housenumber = housenumber, poi_conscriptionnumber = conscriptionnumber, geom_hint = geom_hint, ref = ref)

    def query_all_pd(self, table):
        return pd.read_sql_table(table, self.engine)


def main():
    init_log()
    logging.info('Starting {0} ...'.format(__program__))
    db = POI_Base('postgresql://poi:poitest@localhost:5432')
    logging.info('Importing cities ...'.format())
    '''
    db.add_city('../data/Iranyitoszam-Internet.XLS')

    logging.info('Importing {} stores ...'.format('Tesco'))
    data = [{'poi_code': 'hutescoexp', 'poi_name': 'Tesco Expressz', 'poi_tags':"{'shop': 'convenience', 'operator': 'Tesco Global Áruházak Zrt.', 'brand': 'Tesco', 'contact:website': 'https://www.tesco.hu', 'contact:facebook':'https://www.facebook.com/tescoaruhazak/', 'contact:youtube':'https://www.youtube.com/user/TescoMagyarorszag', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.tesco.hu'},
            {'poi_code': 'hutescoext', 'poi_name': 'Tesco Extra', 'poi_tags': "{'shop': 'supermarket', 'operator': 'Tesco Global Áruházak Zrt.', 'brand': 'Tesco', 'contact:website': 'https://www.tesco.hu', 'contact:facebook':'https://www.facebook.com/tescoaruhazak/', 'contact:youtube':'https://www.youtube.com/user/TescoMagyarorszag', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.tesco.hu'},
            {'poi_code': 'hutescosup', 'poi_name': 'Tesco', 'poi_tags': "{'shop': 'supermarket', 'operator': 'Tesco Global Áruházak Zrt.', 'brand': 'Tesco', 'contact:website': 'https://www.tesco.hu', 'contact:facebook':'https://www.facebook.com/tescoaruhazak/', 'contact:youtube':'https://www.youtube.com/user/TescoMagyarorszag', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.tesco.hu'}]
    db.add_poi_types(data)
    db.add_tesco('http://tesco.hu/aruhazak/nyitvatartas/')

    logging.info('Importing {} stores ...'.format('Aldi'))
    data = [{'poi_code': 'hualdisup', 'poi_name': 'Aldi', 'poi_tags': "{'shop': 'supermarket', 'operator': 'ALDI Magyarország Élelmiszer Bt.', 'brand': 'Aldi', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.aldi.hu'}]
    db.add_poi_types(data)
    db.add_aldi('https://www.aldi.hu/hu/informaciok/informaciok/uezletkereso-es-nyitvatartas/')

    logging.info('Importing {} stores ...'.format('CBA'))
    data = [
        {'poi_code': 'hucbacon', 'poi_name': 'CBA', 'poi_tags': "{'shop': 'convenience', 'brand': 'CBA', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.cba.hu'},
        {'poi_code': 'hucbasup', 'poi_name': 'CBA', 'poi_tags': "{'shop': 'supermarket', 'brand': 'CBA', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.cba.hu'},
        {'poi_code': 'huprimacon', 'poi_name': 'Príma', 'poi_tags': "{'shop': 'convenience', 'brand': 'Príma', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.prima.hu'},
        {'poi_code': 'huprimasup', 'poi_name': 'Príma', 'poi_tags': "{'shop': 'supermarket', 'brand': 'Príma', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.prima.hu'}]
    db.add_poi_types(data)
    db.add_cba('http://www.cba.hu/uzletlista/')

    logging.info('Importing {} stores ...'.format('Spar'))
    data = [{'poi_code': 'husparexp', 'poi_name': 'Spar Expressz', 'poi_tags':"{'shop': 'convenience', 'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.spar.hu'},
            {'poi_code': 'husparint', 'poi_name': 'Interspar', 'poi_tags': "{'shop': 'supermarket', 'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.spar.hu'},
            {'poi_code': 'husparsup', 'poi_name': 'Spar', 'poi_tags': "{'shop': 'supermarket', 'operator': 'SPAR Magyarország Kereskedelmi Kft.', 'brand': 'Spar', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.spar.hu'}]
    db.add_poi_types(data)
    db.add_spar('https://www.spar.hu/bin/aspiag/storefinder/stores?country=HU')

    logging.info('Importing {} stores ...'.format('Rossmann'))
    data = [{'poi_code': 'hurossmche', 'poi_name': 'Rossmann', 'poi_tags': "{'shop': 'chemist', 'operator': 'Rossmann Magyarország Kft.', 'brand':'Rossmann', 'payment:debit_cards': 'yes'}", 'poi_url_base': 'https://www.rossmann.hu'}]
    db.add_poi_types(data)
    db.add_rossmann('https://www.rossmann.hu/uzletkereso')

    logging.info('Importing {} stores ...'.format('KH Bank'))
    data = [{'poi_code': 'hukhbank', 'poi_name': 'K&H bank', 'poi_tags': "{'amenity': 'bank', 'brand': 'K&H', 'operator': 'K&H Bank Zrt.', bic': 'OKHBHUHB', 'atm': 'yes'}", 'poi_url_base': 'https://www.kh.hu'},
            {'poi_code': 'hukhatm', 'poi_name': 'K&H', 'poi_tags': "{'amenity': 'atm', 'brand': 'K&H', 'operator': 'K&H Bank Zrt.'}", 'poi_url_base': 'https://www.kh.hu'}]
    db.add_poi_types(data)
    db.add_kh_bank(os.path.join(DOWNLOAD_CACHE, 'kh_bank.json'), 'K&H bank')
    db.add_kh_bank(os.path.join(DOWNLOAD_CACHE, 'kh_atm.json'), 'K&H')

    logging.info('Importing {} stores ...'.format('BENU'))
    data = [{'poi_code': 'hubenupha', 'poi_name': 'Benu gyógyszertár', 'poi_tags':"{'amenity': 'pharmacy', 'dispensing': 'yes'}", 'poi_url_base': 'https://www.benu.hu'}]
    db.add_poi_types(data)
    db.add_benu('https://benu.hu/wordpress-core/wp-admin/admin-ajax.php?action=asl_load_stores&nonce=1900018ba1&load_all=1&layout=1')

    logging.info('Importing {} stores ...'.format('Magyar Posta'))
    data = [{'poi_code': 'hupostapo', 'poi_name': 'Posta', 'poi_tags':"{'amenity': 'post_office', 'brand': 'Magyar Posta', operator: 'Magyar Posta Zrt.'}", 'poi_url_base': 'https://www.posta.hu'},
            {'poi_code': 'hupostacse', 'poi_name': 'Posta csekkautomata', 'poi_tags': "{'amenity': 'vending_machine', 'brand': 'Magyar Posta', operator: 'Magyar Posta Zrt.'}", 'poi_url_base': 'https://www.posta.hu'},
            {'poi_code': 'hupostacso', 'poi_name': 'Posta csomagautomata', 'poi_tags': "{'amenity': 'vending_machine', 'vending': 'parcel_pickup', 'brand': 'Magyar Posta', operator: 'Magyar Posta Zrt.'}", 'poi_url_base': 'https://www.posta.hu'},
            {'poi_code': 'hupostapp', 'poi_name': 'PostaPont', 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', operator: 'Magyar Posta Zrt.'}", 'poi_url_base': 'https://www.posta.hu'},
            {'poi_code': 'hupostamp', 'poi_name': 'Mobilposta', 'poi_tags': "{'amenity': 'post_office', 'brand': 'Magyar Posta', operator: 'Magyar Posta Zrt.'}", 'poi_url_base': 'https://www.posta.hu'}]
    db.add_poi_types(data)
    db.add_posta('https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=posta', 'posta.json')
    db.add_posta('https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=csekkautomata', 'postacsekkautomata.json')
    db.add_posta('https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postaautomata', 'postaautomata.json')
    db.add_posta('https://www.posta.hu/szolgaltatasok/posta-srv-postoffice/rest/postoffice/list?searchField=&searchText=&types=postapoint', 'postapoint.json')

    logging.info('Importing {} stores ...'.format('Penny Market'))
    data = [{'poi_code': 'hupennysup', 'poi_name': 'Penny Market', 'poi_tags':"{'shop': 'supermarket', 'operator': 'Penny Market Kft.', 'brand': 'Penny Market', 'internet_access': 'wlan', 'internet_access:fee': 'no', 'internet_access:ssid': 'PENNY FREE WLAN', 'contact:email': 'ugyfelszolgalat@penny.hu', 'contact:facebook': 'https://www.facebook.com/PennyMarketMagyarorszag', 'contact:instagram': 'https://www.instagram.com/pennymarkethu/', 'contact:youtube': 'https://www.youtube.com/channel/UCSy0KKUrDxVWkx8qicky_pQ', 'payment:debit_cards': 'yes', 'ref:vatin:hu': '10969629-2-44'}", 'poi_url_base': 'https://www.penny.hu'}]
    db.add_poi_types(data)

    logging.info('Importing {} stores ...'.format('Foxpost'))
    data = [{'poi_code': 'hufoxpocso', 'poi_name': 'Foxpost', 'poi_tags':"{'amenity': 'vending_machine', 'vending': 'parcel_pickup;parcel_mail_in', 'brand': 'Foxpost', operator: 'FoxPost Zrt.', 'contact:facebook': 'https://www.facebook.com/foxpostzrt', 'contact:youtube': 'https://www.youtube.com/channel/UC3zt91sNKPimgA32Nmcu97w', 'contact:email': 'info@foxpost.hu', 'contact:phone': '+36 1 999 03 69', 'payment:debit_cards': 'yes', 'payment:cash': 'no'}", 'poi_url_base': 'https://www.foxpost.hu/'}]
    db.add_poi_types(data)
    db.add_foxpost('http://www.foxpost.hu/wp-content/themes/foxpost/googleapijson.php', 'foxpostautomata.json')
    '''
    logging.info('Importing {} stores ...'.format('Avia'))
    data = [
        {'poi_code': 'huaviafu', 'poi_name': 'Avia', 'poi_tags': "{'amenity': 'fuel', 'brand': 'Avia', 'operator': 'AVIA Hungária Kft.', 'payment:cash': 'yes', 'payment:debit_cards': 'yes', 'fuel:diesel': 'yes', 'fuel:octane_95': 'yes'}", 'poi_url_base': 'https://www.avia.hu'}]
    db.add_poi_types(data)
    db.add_avia('https://www.avia.hu/kapcsolat/toltoallomasok/')

    '''
    logging.info('Importing {} stores ...'.format('CIB Bank'))
    data = [{'poi_code': 'hucibbank', 'poi_name': 'CIB bank', 'poi_tags': "{'amenity': 'bank', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.', bic': 'CIBHHUHB', 'atm': 'yes'}", 'poi_url_base': 'https://www.cib.hu/elerhetosegek/fiokok_bankautomatak/index'},
            {'poi_code': 'hucibatm', 'poi_name': 'CIB', 'poi_tags': "{'amenity': 'atm', 'brand': 'CIB', 'operator': 'CIB Bank Zrt.'}", 'poi_url_base': 'https://www.cib.hu/elerhetosegek/fiokok_bankautomatak/index'}]
    db.add_poi_types(data)
    db.add_cib_bank(os.path.join(DOWNLOAD_CACHE, 'cib_bank.html'), 'CIB bank')
    db.add_cib_bank(os.path.join(DOWNLOAD_CACHE, 'cib_atm.html'), 'CIB')
    '''
    logging.info('Exporting CSV files ...')
    targets = ['poi_address', 'poi_common']
    for i in targets:
        data = db.query_all_pd(i)
        save_csv_file(output_folder, '{}.csv'.format(i), data, i)

if __name__ == '__main__':
    main()
