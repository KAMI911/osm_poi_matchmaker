# -*- coding: utf-8 -*-

try:
    import traceback
    import logging
    import sys
    import re
    import phonenumbers
    import json
    from functools import reduce
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.error(traceback.print_exc())
    sys.exit(128)

# Patterns for re
PATTERN_POSTCODE_CITY = re.compile('^((\d){4})([.\s]{0,2})([a-zA-ZáÁéÉíÍóÓúÚüÜöÖőŐűŰ]{3,40})')
PATTERN_CITY_ADDRESS = re.compile('^([a-zA-ZáÁéÉíÍóÓúÚüÜöÖőŐűŰ]{3,40})')
PATTERN_CITY = re.compile('\s?[XVI]{1,5}[.:,]{0,3}\s*$')
PATTERN_JS_2 = re.compile('\s*;\s*$')
PATTERN_HOUSENUMBER = re.compile('[0-9]{1,3}(\/[A-z]{1}|\-[0-9]{1,3}|)', re.IGNORECASE)
PATTERN_CONSCRIPTIONNUMBER = re.compile(
    '(hrsz[.:]{0,2}\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1}|\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1}\s*hrsz[s.]{0,1})',
    re.IGNORECASE)
PATTERN_CONSCRIPTIONNUMBER_1 = re.compile('(hrsz[.:]{0,2}\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1})', re.IGNORECASE)
PATTERN_CONSCRIPTIONNUMBER_2 = re.compile('(\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1}\s*hrsz[s.]{0,1})', re.IGNORECASE)
PATTERN_OPENING_HOURS = re.compile('0*[0-9]{1,2}\:0*[0-9]{1,2}\s*-\s*0*[0-9]{1,2}:0*[0-9]{1,2}')
PATTERN_PHONE = re.compile('[\/\\\(\)\-\+]')

PATTERN_STREET_RICH = re.compile(
    '\s*(.*)\s+(akna|alja|almáskert|alsó|alsósor|aluljáró|autópálya|autóversenypálya|állomás|árok|átjáró|barakképület|bánya|bányatelep|bekötőút|benzinkút|bérc|bisztró|bokor|burgundia|büfé|camping|campingsor|centrum|célgazdaság|csapás|csarnok|csárda|cser|csoport|domb|dunapart|dunasor|dűlő|dűlője|dűlők|dűlőút|egyesület|egyéb|elágazás|erdeje|erdészház|erdészlak|erdő|erdősarok|erdősor|épület|épületek|észak|étterem|falu|farm|fasor|fasora|feketeerdő|feketeföldek|felső|felsősor|fennsík|fogadó|fok|forduló|forrás|föld|földek|földje|főcsatorna|főtér|főút|fürdő|fürdőhely|fürésztelepe|gazdaság|gát|gátőrház|gátsor|gimnázium|gödör|gulyakút|gyár|gyártelep|halom|határátkelőhely|határrész|határsor|határút|hatházak|hát|ház|háza|házak|hegy|hegyhát|hegyhát dűlő|hely|hivatal|híd|hídfő|horgásztanya|hotel|intézet|ipari park|ipartelep|iparterület|irodaház|irtás|iskola|jánoshegy|járás|juhászház|kapcsolóház|kapu|kastély|kálvária|kemping|kert|kertek|kertek-köze|kertsor|kertváros|kerület|kikötő|kilátó|kishajtás|kitérő|kocsiszín|kolónia|korzó|kórház|környék|körönd|körtér|körút|körútja|körvasútsor|körzet|köz|köze|középsor|központ|kút|kútház|kültelek|külterület|külterülete|lakás|lakások|lakóház|lakókert|lakónegyed|lakópark|lakótelep|laktanya|legelő|lejáró|lejtő|lépcső|liget|lovasiskola|lovastanya|magánút|major|malom|malomsor|megálló|mellékköz|mező|mélyút|MGTSZ|munkásszálló|műút|nagymajor|nagyút|nádgazdaság|nyaraló|oldal|országút|otthon|otthona|öböl|öregszőlők|ösvény|ötház|őrház|őrházak|pagony|pallag|palota|park|parkfalu|parkja|parkoló|part|pavilonsor|pálya|pályafenntartás|pályaudvar|piac|pihenő|pihenőhely|pince|pinceköz|pincesor|pincék|présházak|puszta|rakodó|rakpart|repülőtér|rész|rét|rétek|rév|ring|sarok|sertéstelep|sétatér|sétány|sikátor|sor|sora|sportpálya|sporttelep|stadion|strand|strandfürdő|sugárút|szabadstrand|szakiskola|szállás|szálló|szárító|szárnyasliget|szektor|szer|szél|széle|sziget|szigete|szivattyútelep|szög|szőlő|szőlőhegy|szőlők|szőlőkert|szőlős|szőlősor|tag|tanya|tanyaközpont|tanyasor|tanyák|tavak|tábor|tároló|társasház|teherpályaudvar|telek|telep|telepek|település|temető|tere|terményraktár|terület|teteje|tető|téglagyár|tér|tipegő|tormás|torony|tó|tömb|TSZ|turistaház|udvar|udvara|ugarok|utca|utcája|újfalu|újsor|újtelep|útfél|útgyűrű|útja|út|üdülő|üdülő központ|üdülő park|üdülők|üdülőközpont|üdülőpart|üdülő-part|üdülősor|üdülő-sor|üdülőtelep|üdülő-telep|üdülőterület|ürbő|üzem|üzletház|üzletsor|vadászház|varroda|vasútállomás|vasúti megálló|vasúti őrház|vasútsor|vám|vár|város|városrész|vásártér|vendéglő|vég|villa|villasor|viztároló|vízmű|vízmű telep|völgy|zsilip|zug|ltp\.|ltp|krt\.|krt|sgt\.|u\.|u\s+|Várkerület){1}.*',
    re.UNICODE | re.IGNORECASE)
PATTERN_URL_SLASH = re.compile('(?<!:)(//{1,})')
PATTERN_FULL_URL = re.compile('((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)')


def clean_javascript_variable(clearable, removable):
    """Remove javascript variable notation from the selected JSON variable.

    :param clearable: This is the text with Javascript text
    :param removable: The name of Javascript variable
    :return: Javascript clean text/JSON file
    """
    # Match on start
    PATTERN_JS = re.compile('^\s*var\s*{}\s*=\s*'.format(removable))
    data = re.sub(PATTERN_JS, '', clearable)
    # Match on end
    return re.sub(PATTERN_JS_2, '', data)


def extract_javascript_variable(input_soup, removable, use_replace=False):
    """Extract JavaScript variable from <script> tag from a soup.

    :param sp: Input soup
    :param removable: The name of Javascript variable
    :param user_replace: Additional step to replace from ' to "
    :return: Javascript clean text/JSON file
    """
    # Match on start
    try:
        pattern = re.compile('.*\s*var\s*{}\s*=\s*(.*?[}}\]]);'.format(removable), re.MULTILINE | re.DOTALL)
        script = str(input_soup.find('script', text=pattern))
        if use_replace is True: script = script.replace("'", '"')
        m = pattern.match(script)
        try:
            if m is not None:
                return m.group(1)
            else:
                return None
        except AttributeError as e:
            logging.warning('An exception has occured during JavaScript variable extraction.')
    except Exception as e:
        logging.error(e)
        logging.error(traceback.print_exc())
        logging.error(pattern)
        logging.error(script)
        logging.error(m.group(1))


def extract_street_housenumber(clearable):
    '''Try to separate street and house number from a Hungarian style address string

    :param clearable: An input string with Hungarian style address string
    return: Separated street and housenumber
    '''
    # Split and clean up house number
    housenumber = clearable.split('(')[0]
    housenumber = housenumber.split(' ')[-1]
    housenumber = housenumber.replace('.', '')
    housenumber = housenumber.replace('–', '-')
    housenumber = housenumber.upper()
    # Split and clean up street
    street = clearable.split('(')[0]
    street = street.rsplit(' ', 1)[0]
    street = street.replace(' u.', ' utca')
    street = street.replace(' u ', ' utca')
    street = street.replace(' krt.', ' körút')
    return street, housenumber


def extract_all_address(clearable):
    if clearable is not None and clearable != '':
        clearable = clearable.strip()
        pc_match = PATTERN_POSTCODE_CITY.search(clearable)
        if pc_match is not None:
            postcode = pc_match.group(1)
        else:
            postcode = None
        if pc_match is not None:
            city = pc_match.group(4)
        else:
            city = None
        if len(clearable.split(',')) > 1:
            street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(
                clearable.split(',')[1].strip())
            return (postcode, city, street, housenumber, conscriptionnumber)
        else:
            space_separated = ' '.join(clearable.split(' ')[2:]).strip()
            street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(space_separated)
            return (postcode, city, street, housenumber, conscriptionnumber)
    else:
        return None, None, None, None, None


def extract_city_street_housenumber_address(clearable):
    if clearable is not None and clearable != '':
        clearable = clearable.strip()
        pc_match = PATTERN_CITY_ADDRESS.search(clearable)
        if pc_match is not None:
            city = pc_match.group(1)
        else:
            city = None
        if len(clearable.split(',')) > 1:
            street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(
                clearable.split(',')[1].strip())
            return (city, street, housenumber, conscriptionnumber)
        else:
            return city, None, None, None
    else:
        return None, None, None, None, None


def extract_street_housenumber_better_2(clearable):
    '''Try to separate street and house number from a Hungarian style address string

    :param clearable: An input string with Hungarian style address string
    return: Separated street and housenumber
    '''
    # Split and clean up street
    if clearable is not None and clearable.strip() != '':
        clearable = clearable.strip()
        # Remove bulding names
        clearable = clearable.replace(' - Savoya Park', '')
        clearable = clearable.replace('Park Center,', '')
        clearable = clearable.replace('Duna Center', '')
        clearable = clearable.replace('Family Center,', '')
        clearable = clearable.replace('Sostói ipari park, ', '')
        data = clearable.split('(')[0]
        cn_match_1 = PATTERN_CONSCRIPTIONNUMBER_1.search(data)
        cn_match_2 = PATTERN_CONSCRIPTIONNUMBER_2.search(data)
        if cn_match_1 is not None:
            conscriptionnumber = cn_match_1.group(2) if cn_match_1.group(2) is not None else None
            cnn_length = len(cn_match_1.group(0))
            logging.debug(
                'Matching conscription number with method 1: {} from {}'.format(conscriptionnumber, clearable))
        elif cn_match_2 is not None:
            conscriptionnumber = cn_match_2.group(2) if cn_match_2.group(2) is not None else None
            cnn_length = len(cn_match_2.group(0))
            logging.debug(
                'Matching conscription number with method 2: {} from {}'.format(conscriptionnumber, clearable))
        else:
            conscriptionnumber = None
            cnn_length = None
        # Try to match street
        street_corrected = clean_street(data)
        street_match = PATTERN_STREET_RICH.search(street_corrected)
        if street_match is None:
            logging.debug('Non matching street: {}'.format(clearable))
            street, housenumber = None, None
        else:
            # Normalize street
            street = street_match.group(1)
            street_type = street_match.group(2)
            # Usually street_type is lower but we got few exceptions
            if street_type not in ['Vám']:
                street_type = street_type.lower()
            street_length = len(street) + len(street_type)
            # Search for house number
            if cnn_length is not None:
                hn_match = PATTERN_HOUSENUMBER.search(street_corrected[street_length:-cnn_length])
            else:
                hn_match = PATTERN_HOUSENUMBER.search(street_corrected[street_length:])
            if hn_match is not None:
                # Split and clean up house number
                housenumber = hn_match.group(0)
                housenumber = housenumber.replace('.', '')
                housenumber = housenumber.replace('–', '-')
                housenumber = housenumber.upper()
            else:
                housenumber = None
        if 'street_type' in locals():
            return '{} {}'.format(street, street_type).strip(), housenumber, conscriptionnumber
        else:
            return street, housenumber, conscriptionnumber
    else:
        return None, None, None


def clean_city(clearable):
    '''Remove additional things from cityname

    :param clearable: Not clear cityname
    :return: Clear cityname
    '''
    if clearable is not None:
        city = re.sub(PATTERN_CITY, '', clearable)
        repls = ('Mikolc', 'Miskolc'), ('Iinárcs', 'Inárcs')
        city = reduce(lambda a, kv: a.replace(*kv), repls, city)
        city = city.split('-')[0]
        city = city.split(',')[0]
        city = city.split('/')[0]
        city = city.split('/')[0]
        city = city.split('(')[0]
        city = city.split(' ')[0]
        return city.title().strip()
    else:
        return None


def clean_opening_hours(oh_from_to):
    oh_match = PATTERN_OPENING_HOURS.search(oh_from_to)
    if oh_match is not None:
        tmp = oh_match.group(0)
    else:
        # No match return None
        return None, None
    # Remove all whitespaces
    tmp = ''.join(tmp.split())
    # We expect exactly two parts, for example: 09:40
    if len(tmp.split('-')) == 2:
        tmf = tmp.split('-')[0].zfill(5)
        tmt = tmp.split('-')[1].zfill(5)
    else:
        tmf, tmt = None, None
    return tmf, tmt


def clean_opening_hours_2(oh):
    if oh == '-1':
        return None
    else:
        tmp = oh.strip().zfill(4)
        fmt = '{}:{}'.format(tmp[:2], tmp[-2:])
    return fmt


def clean_phone(phone):
    # Remove all whitespaces
    original = phone
    if '(' in phone:
        phone = phone.split('(')[0]
    phone = phone.replace('-', ' ')
    if ',' in phone:
        phone = phone.replace(',', ';')
    if ';' in phone:
        phone = phone.split(';')
    try:
        if type(phone) is list:
            for i in phone:
                pn = [phonenumbers.parse(i, 'HU') for i in phone]
        else:
            pn = []
            pn.append(phonenumbers.parse(phone, 'HU'))
    except phonenumbers.phonenumberutil.NumberParseException:
        logging.debug('This is string is cannot converted to phone number: {}'.format(phone))
        return None
    if pn is not None:
        return [ phonenumbers.format_number(i, phonenumbers.PhoneNumberFormat.INTERNATIONAL) for i in pn ]
    else:
        return None


def clean_phone_to_json(phone):
    cleaned = clean_phone(phone)
    if cleaned is not None:
        return json.dumps(cleaned)
    else:
        return None


def clean_phone_to_str(phone):
    cleaned = clean_phone(phone)
    if cleaned is not None:
        return ';'.join(cleaned)
    else:
        return None


def clean_email(email):
    # Remove all whitespaces
    if ',' in email:
        email = email.split(',')[0]
    if ';' in email:
        email = email.split(';')[0]
    return email


def clean_string(clearable):
    '''
    Remove extra spaces from strings and surrounding whitespace characters
    :param clearable: String that has to clean
    :return: Cleaned string
    '''
    if clearable is not None:
        clearable = clearable.replace('  ', ' ').strip()
    return clearable


def clean_url(clearable):
    '''
    Remove extra slashes from URL strings and surrounding whitespace characters
    :param clearable: String that has to clean
    :return: Cleaned string
    '''
    if clearable is not None:
        url_match = PATTERN_URL_SLASH.sub('/', clearable)
        return url_match.strip()
    else:
        return None

def clean_street(clearable):
    '''

    :param clearable:
    :return:
    '''

    street = clearable.strip()
    repls = ('Nyúl 82. sz. főút', 'Kossuth Lajos út'), \
    ('Nyúl  82. sz. főút', '82. számú főközlekedési út'), \
    ('Budafoki út, 6-os sz. főút', '6. számú főközlekedési út'), \
    ('. Sz. Főút felső', '. számú főközlekedési út'), \
    ('. számú - Némedi út sarok', '. számú főközlekedési út'), \
    ('076/15. hrsz 86. számú főút mellett', '86. számú főközlekedési út'), \
    ('50.sz.út jobb oldal', '50. számú főközlekedési út'), \
    ('. sz. fkl.út', '. számú főközlekedési út'), \
    ('.sz. fkl. út', '. számú főközlekedési út'), \
    ('-es sz. főút', '. számú főközlekedési út'), \
    ('. sz. főút', '. számú főközlekedési út'), \
    ('.sz.fkl.', '. számú főközlekedési'), \
    ('. sz. fkl.', '. számú főközlekedési'), \
    ('. számú fkl. út', '. számú főközlekedési út'), \
    ('. Sz. főút', '. számú főközlekedési út'), \
    ('. számú főút', '. számú főközlekedési út'), \
    ('. főút', '. számú főközlekedési út'), \
    ('. sz út', '. számú főközlekedési út'), \
    (' sz. főút', '. számú főközlekedési út'), \
    ('-es fő út', '. számú főközlekedési út'), \
    ('-es főút', '. számú főközlekedési út'), \
    (' - es út', '. számú főközlekedési út'), \
    ('-es út', '. számú főközlekedési út'), \
    ('-as fő út', '. számú főközlekedési út'), \
    ('-as főút', '. számú főközlekedési út'), \
    (' - as út', '. számú főközlekedési út'), \
    ('-as út', '. számú főközlekedési út'), \
    ('-ös fő út', '. számú főközlekedési út'), \
    ('-ös főút', '. számú főközlekedési út'), \
    (' - ös út', '. számú főközlekedési út'), \
    ('-ös út', '. számú főközlekedési út'), \
    ('Omsz park', 'Omszk park'), \
    ('01.máj.', 'Május 1.'), \
    ('15.márc.', 'Március 15.'), \
    ('Ady E.', 'Ady Endre'), \
    ('Áchim A.', 'Áchim András'), \
    ('Bajcsy-Zs. E.', 'Bajcsy-Zsilinszky Endre'), \
    ('Bajcsy-Zs. E. u.', 'Bajcsy-Zsilinszky Endre utca'), \
    ('Bajcsy-Zs. u.', 'Bajcsy-Zsilinszky utca'), \
    ('Bajcsy Zs.u.', 'Bajcsy-Zsilinszky utca'), \
    ('Bajcsy-Zs. u.', 'Bajcsy-Zsilinszky utca'), \
    ('Bajcsy Zs. u.', 'Bajcsy-Zsilinszky utca'), \
    ('Bajcsy-Zs.', 'Bajcsy-Zsilinszky'), \
    ('Bajcsy Zs.', 'Bajcsy-Zsilinszky'), \
    ('Bartók B.', 'Bartók Béla'), \
    ('Baross G.', 'Baross Gábor'), \
    ('BERCSÉNYI U.', 'Bercsényi Miklós utca'), \
    ('Berzsenyi D.', 'Berzsenyi Dániel'), \
    ('Borics P.', 'Borics Pál'), \
    ('Corvin J.', 'Corvin'), \
    ('Dózsa Gy.u.', 'Dózsa György utca'), \
    ('Dózsa Gy.', 'Dózsa György'), \
    ('dr. Géfin Lajos', 'Dr. Géfin Lajos'), \
    ('Erkel F.', 'Erkel Ferenc'), \
    ('Hegedű/(Király)', 'Hegedű'), \
    ('Hevesi S.', 'Hevesi Sándor'), \
    ('Hunyadi J.', 'Hunyadi János'), \
    ('Ii. Rákóczi Ferenc', 'II. Rákóczi Ferenc'), \
    ('Innovációs kp. Fő út', 'Fő út'), \
    ('Ix. körzet', 'IX. körzet'), \
    ('Kölcsey F.', 'Kölcsey Ferenc'), \
    ('Kiss J.', 'Kiss József'), \
    ('Nagy L. király', 'Nagy Lajos király'), \
    ('Kaszás u. 2.-Dózsa György út', 'Dózsa György út'), \
    ('Váci út 117-119. „A” épület', 'Váci út'), \
    ('56-Osok tere', 'Ötvenhatosok tere'), \
    ('11-es út', '11. számú főközlekedési út'), \
    ('11-es Huszár út', 'Huszár út'), \
    ('Kölcsey-Pozsonyi út sarok', 'Kölcsey Ferenc utca '), \
    ('Március 15-e', 'Március 15.'), \
    ('Tiszavasvári út - Alkotás u sarok', 'Tiszavasvári út'), \
    ('Tiszavasvári út- Alkotás út sarok', 'Tiszavasvári út'), \
    ('Hőforrás-Rákóczi utca', 'Rákóczi utca'), \
    ('Kiss Tábornok - Kandó Kálmán utca sarok', 'Kiss Tábornok utca'), \
    ('Soroksári út - Határ út sarok', 'Soroksári út'), \
    ('Szentendrei- Czetz János utca sarok', 'Szentendrei út'), \
    ('Külső - Kádártai utca', 'Külső-Kádártai utca'), \
    ('Károlyi út - Ságvári út', 'Károlyi Mihály utca'), \
    ('Szlovák út - Csömöri út sarok', 'Szlovák út'), \
    ('Maglódi út - Jászberényi út sarok', 'Maglódi út'), \
    ('Dobogókői út- Kesztölci út sarok', 'Dobogókői út'), \
    ('DR. KOCH L. UTCA', 'Dr. Koch László utca'), \
    ('DR KOCH L.', 'Dr. Koch László'), \
    ('Koch L.u.', 'Dr. Koch László utca'), \
    ('Kiss J. ', 'Kiss József'), \
    ('Kossuth L.u.', 'Kossuth Lajos utca '), \
    ('Kossuth L.', 'Kossuth Lajos'), \
    ('Kossuth F. u', 'Kossuth Ferenc utca'), \
    ('Kossuth F.', 'Kossuth Ferenc'), \
    ('Korányi F.', 'Korányi Frigyes'), \
    ('Kőrösi Csoma S.', 'Kőrösi Csoma Sándor'), \
    ('Páter K.', 'Páter Károly'), \
    ('Petőfi S.', 'Petőfi Sándor'), \
    ('Somogyi B.', 'Somogyi Béla'), \
    ('Szondy', 'Szondi'), \
    ('Szt.István', 'Szent István'), \
    ('szt.istván', 'Szent István'), \
    ('Táncsics M.', 'Táncsics Mihály'), \
    ('Vass J.', 'Vass János'), \
    ('Vámház.', 'Vámház'), \
    ('Várkörút .', 'Várkörút'), \
    ('Vásárhelyi P.', 'Vásárhelyi Pál'), \
    ('Vi. utca', 'VI. utca'), \
    ('XXI. II. Rákóczi Ferenc', 'II. Rákóczi Ferenc'), \
    ('Zsolnay V.', 'Zsolnay Vilmos'), \
    ('Radnóti M.', 'Radnóti Miklós'), \
    ('Fehérvári út (Andor u. 1.)', 'Fehérvári'), \
    ('Szent István kir.', 'Szent István király'), \
    ('Dr Batthyány S. László', 'Dr. Batthyány-Strattmann László'), \
    ('Bacsinszky A.', 'Bacsinszky András'), \
    ('Fáy A.', 'Fáy András'), \
    ('József a.', 'József Attila'), \
    ('Juhász Gy. ', 'Juhász Gyula'), \
    ('Hock j.', 'Hock János'), \
    ('Vak B.', 'Vak Bottyán'), \
    ('Arany J.', 'Arany János'), \
    ('Könyves K.', 'Könyves Kálmán'), \
    ('Szilágyi E.', 'Szilágyi Erzsébet'), \
    ('Liszt F.', 'Liszt Ferenc'), \
    ('Bethlen G.', 'Bethlen Gábor'), \
    ('Gazdag E.', 'Gazdag Erzsi'), \
    ('Hátsókapu.', 'Hátsókapu'), \
    ('Herman O.', 'Herman Ottó'), \
    ('József A.', 'József Attila'), \
    ('Kazinczy F.', 'Kazinczy Ferenc'), \
    ('Király J.', 'Király Jenő'), \
    ('Királyhidai utca', 'Királyhidai út'), \
    ('Lackner K.', 'Lackner Kristóf'), \
    ('Mécs L.', 'Mécs László'), \
    ('Nagyváthy J.', 'Nagyváthy János'), \
    ('Szent I. kir.', 'Szent István király'), \
    ('Szigethy A. u.', 'Szigethy Attila út'), \
    ('Rákóczi F.', 'Rákóczi Ferenc'), \
    ('Jókai M.', 'Jókai Mór'), \
    ('Szabó D.', 'Szabó Dezső'), \
    ('Kossuth F.', 'Kossuth F.'), \
    ('Móricz Zs.', 'Móricz Zsigmond'), \
    ('Hunyadi J ', 'Hunyadi János'), \
    ('Szilágyi E ', 'Szilágyi Erzsébet fasor'), \
    ('Erzsébet Királyné út', 'Erzsébet királyné útja'), \
    ('Mammut', ''), \
    ('Szt. ', 'Szent '), \
    (' u.', ' utca '), \
    (' U.', ' utca '), \
    ('.u.', ' utca '), \
    (' u ', ' utca '), \
    (' krt.', ' körút'), \
    (' Krt.', ' körút'), \
    (' KRT.', ' körút'), \
    (' ltp.', ' lakótelep'), \
    (' Ltp.', ' lakótelep'), \
    (' LTP.', ' lakótelep'), \
    (' ltp', ' lakótelep'), \
    (' sgt.', ' sugárút'), \
    ('^4. sz$', '4. számú főközlekedési')
    street = reduce(lambda a, kv: a.replace(*kv), repls, street)
    return street

def clean_street_type(clearable):
    '''

    :param clearable:
    :return:
    '''

    street = clearable.replace('fkl. út', 'főközlekedési út')
    street = street.replace('főút', 'főközlekedési út')
    street = street.replace('ltp.', ' lakótelep')
    street = street.replace('LTP.', ' lakótelep')
    street = street.replace('pu.', 'pályaudvar')
    street = street.replace('út.', 'út')
    street = street.replace('u.', 'utca')
    street = street.replace('(nincs)', '')
    street = street.replace('.', '')
    return street

def clean_branch(clearable):
    '''

    :param clearable:
    :return:
    '''
    if clearable is not None and clearable != '':
        clearable = clearable.strip()
        branch = clearable.replace('sz.', 'számú')
        return branch
    else:
        return None
