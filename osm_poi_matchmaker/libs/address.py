# -*- coding: utf-8 -*-

try:
    import re
    import logging
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)

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
            return postcode, city, None, None, None
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
    clearable = clearable.strip()
    if clearable is not None and clearable != '':
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
        city = city.split('-')[0]
        city = city.split(',')[0]
        city = city.split('/')[0]
        city = city.split('/')[0]
        city = city.split('(')[0]
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
    if ',' in phone:
        phone = phone.split(',')[0]
    if ';' in phone:
        phone = phone.split(';')[0]
    if '(' in phone:
        phone = phone.split('(')[0]
    phone = ''.join(phone.split())
    ph_match = PATTERN_PHONE.sub('', phone)
    if ph_match[:2] == '06':
        ph_match = '36{}'.format(ph_match[2:])
    if len(ph_match) == 8:
        ph_match = '36{}'.format(ph_match)
    if ph_match != '':
        return ph_match
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
        url_match = PATTERN_URL_SLASH.sub('/' , clearable)
        return url_match.strip()
    else:
        return None

def clean_street(clearable):
    '''

    :param clearable:
    :return:
    '''

    street = clearable.replace('Nyúl 82. sz. főút', 'Kossuth Lajos út')
    street = street.replace('Nyúl  82. sz. főút', '82. számú főközlekedési út')
    street = street.replace('Budafoki út, 6-os sz. főút', '6. számú főközlekedési út')
    street = street.replace('. Sz. Főút felső', '. számú főközlekedési út')
    street = street.replace('. számú - Némedi út sarok', '. számú főközlekedési út')
    street = street.replace('076/15. hrsz 86. számú főút mellett', '86. számú főközlekedési út')
    street = street.replace('50.sz.út jobb oldal', '50. számú főközlekedési út')

    street = street.replace('. sz. fkl.út', '. számú főközlekedési út')
    street = street.replace('.sz. fkl. út', '. számú főközlekedési út')
    street = street.replace('-es sz. főút', '. számú főközlekedési út')
    street = street.replace('. sz. főút', '. számú főközlekedési út')
    street = street.replace('.sz.fkl.', '. számú főközlekedési')
    street = street.replace('. sz. fkl.', '. számú főközlekedési')
    street = street.replace('. számú fkl. út', '. számú főközlekedési út')
    street = street.replace('. Sz. főút', '. számú főközlekedési út')
    street = street.replace('. számú főút', '. számú főközlekedési út')
    street = street.replace('. főút', '. számú főközlekedési út')
    street = street.replace('. sz út', '. számú főközlekedési út')
    street = street.replace(' sz. főút', '. számú főközlekedési út')

    street = street.replace('-es fő út', '. számú főközlekedési út')
    street = street.replace('-es főút', '. számú főközlekedési út')
    street = street.replace(' - es út', '. számú főközlekedési út')
    street = street.replace('-es út', '. számú főközlekedési út')
    street = street.replace('-as fő út', '. számú főközlekedési út')
    street = street.replace('-as főút', '. számú főközlekedési út')
    street = street.replace(' - as út', '. számú főközlekedési út')
    street = street.replace('-as út', '. számú főközlekedési út')
    street = street.replace('-ös fő út', '. számú főközlekedési út')
    street = street.replace('-ös főút', '. számú főközlekedési út')
    street = street.replace(' - ös út', '. számú főközlekedési út')
    street = street.replace('-ös út', '. számú főközlekedési út')

    street = street.replace('01.máj.', 'Május 1.')
    street = street.replace('15.márc.', 'Március 15.')
    street = street.replace('Ady E.', 'Ady Endre')
    street = street.replace('Áchim A.', 'Áchim András')
    street = street.replace('Bajcsy-Zs. E.', 'Bajcsy-Zsilinszky Endre')
    street = street.replace('Bajcsy-Zs. E. u.', 'Bajcsy-Zsilinszky Endre utca')
    street = street.replace('Bajcsy-Zs. u.', 'Bajcsy-Zsilinszky Endre utca')
    street = street.replace('Bajcsy Zs.u.', 'Bajcsy-Zsilinszky Endre utca')
    street = street.replace('Bajcsy-Zs.', 'Bajcsy-Zsilinszky Endre')
    street = street.replace('Bajcsy Zs.', 'Bajcsy-Zsilinszky Endre')
    street = street.replace('Bartók B.', 'Bartók Béla')
    street = street.replace('Baross G.', 'Baross Gábor')
    street = street.replace('BERCSÉNYI U.', 'Bercsényi Miklós utca')
    street = street.replace('Berzsenyi D.', 'Berzsenyi Dániel')
    street = street.replace('Borics P.', 'Borics Pál')
    street = street.replace('Corvin J.', 'Corvin')
    street = street.replace('Dózsa Gy.u.', 'Dózsa György utca')
    street = street.replace('Dózsa Gy.', 'Dózsa György')
    street = street.replace('dr. Géfin Lajos', 'Dr. Géfin Lajos')
    street = street.replace('Erkel F.', 'Erkel Ferenc')
    street = street.replace('Hegedű/(Király)', 'Hegedű')
    street = street.replace('Hevesi S.', 'Hevesi Sándor')
    street = street.replace('Hunyadi J.', 'Hunyadi János')
    street = street.replace('Ii. Rákóczi Ferenc', 'II. Rákóczi Ferenc')
    street = street.replace('Innovációs kp. Fő út', 'Fő út')
    street = street.replace('Ix. körzet', 'IX. körzet')
    street = street.replace('Kölcsey F.', 'Kölcsey Ferenc')
    street = street.replace('Kiss J.', 'Kiss József')
    street = street.replace('Nagy L. király', 'Nagy Lajos király')
    street = street.replace('Kaszás u. 2.-Dózsa György út', 'Dózsa György út')
    street = street.replace('Váci út 117-119. „A” épület', 'Váci út')
    street = street.replace('56-Osok tere', 'Ötvenhatosok tere')
    street = street.replace('11-es út', '11. számú főközlekedési út')
    street = street.replace('11-es Huszár út', 'Huszár út')
    street = street.replace('Kölcsey-Pozsonyi út sarok', 'Kölcsey Ferenc utca ')
    street = street.replace('Március 15-e', 'Március 15.')
    street = street.replace('Tiszavasvári út - Alkotás u sarok', 'Tiszavasvári út')
    street = street.replace('Tiszavasvári út- Alkotás út sarok', 'Tiszavasvári út')
    street = street.replace('Hőforrás-Rákóczi utca', 'Rákóczi utca')
    street = street.replace('Kiss Tábornok - Kandó Kálmán utca sarok', 'Kiss Tábornok utca')
    street = street.replace('Soroksári út - Határ út sarok', 'Soroksári út')
    street = street.replace('Szentendrei- Czetz János utca sarok', 'Szentendrei út')
    street = street.replace('Külső - Kádártai utca', 'Külső-Kádártai utca')
    street = street.replace('Károlyi út - Ságvári út', 'Károlyi Mihály utca')
    street = street.replace('Szlovák út - Csömöri út sarok', 'Szlovák út')
    street = street.replace('Maglódi út - Jászberényi út sarok', 'Maglódi út')
    street = street.replace('Dobogókői út- Kesztölci út sarok', 'Dobogókői út')
    street = street.replace('DR. KOCH L. UTCA', 'Dr. Koch László utca')
    street = street.replace('DR KOCH L.', 'Dr. Koch László')
    street = street.replace('Koch L.u.', 'Dr. Koch László utca')
    street = street.replace('Kiss J. ', 'Kiss József')
    street = street.replace('Kossuth L.u.', 'Kossuth Lajos utca ')
    street = street.replace('Kossuth L.', 'Kossuth Lajos')
    street = street.replace('Kossuth F. u', 'Kossuth Ferenc utca')
    street = street.replace('Kossuth F.', 'Kossuth Ferenc')
    street = street.replace('Korányi F.', 'Korányi Frigyes')
    street = street.replace('Kőrösi Csoma S.', 'Kőrösi Csoma Sándor')
    street = street.replace('Páter K.', 'Páter Károly')
    street = street.replace('Petőfi S.', 'Petőfi Sándor')
    street = street.replace('Somogyi B.', 'Somogyi Béla')
    street = street.replace('Szondy', 'Szondi')
    street = street.replace('Szt.István', 'Szent István')
    street = street.replace('szt.istván', 'Szent István')
    street = street.replace('Táncsics M.', 'Táncsics Mihály')
    street = street.replace('Vass J.', 'Vass János')
    street = street.replace('Vámház.', 'Vámház')
    street = street.replace('Várkörút .', 'Várkörút')
    street = street.replace('Vásárhelyi P.', 'Vásárhelyi Pál')
    street = street.replace('Vi. utca', 'VI. utca')
    street = street.replace('XXI. II. Rákóczi Ferenc', 'II. Rákóczi Ferenc')
    street = street.replace('Zsolnay V.', 'Zsolnay Vilmos')
    street = street.replace('Radnóti M.', 'Radnóti Miklós')
    street = street.replace('Fehérvári út (Andor u. 1.)', 'Fehérvári')
    street = street.replace('Szent István kir.', 'Szent István király')
    street = street.replace('Dr Batthyány S. László', 'Dr. Batthyány-Strattmann László')
    street = street.replace('Bacsinszky A.', 'Bacsinszky András')
    street = street.replace('Fáy A.', 'Fáy András')
    street = street.replace('József a.', 'József Attila')
    street = street.replace('Juhász Gy. ', 'Juhász Gyula')
    street = street.replace('Hock j.', 'Hock János')
    street = street.replace('Vak B.', 'Vak Bottyán')
    street = street.replace('Arany J.', 'Arany János')
    street = street.replace('Könyves K.', 'Könyves Kálmán')
    street = street.replace('Szilágyi E.', 'Szilágyi Erzsébet')
    street = street.replace('Liszt F.', 'Liszt Ferenc')
    street = street.replace('Bethlen G.', 'Bethlen Gábor')
    street = street.replace('Gazdag E.', 'Gazdag Erzsi')
    street = street.replace('Hátsókapu.', 'Hátsókapu')
    street = street.replace('Herman O.', 'Herman Ottó')
    street = street.replace('József A.', 'József Attila')
    street = street.replace('Kazinczy F.', 'Kazinczy Ferenc')
    street = street.replace('Király J.', 'Király Jenő')
    street = street.replace('Királyhidai utca', 'Királyhidai út')
    street = street.replace('Lackner K.', 'Lackner Kristóf')
    street = street.replace('Mécs L.', 'Mécs László')
    street = street.replace('Nagyváthy J.', 'Nagyváthy János')
    street = street.replace('Szent I. kir.', 'Szent István király')
    street = street.replace('Szigethy A. u.', 'Szigethy Attila út')
    street = street.replace('Rákóczi F.', 'Rákóczi Ferenc')
    street = street.replace('Jókai M.', 'Jókai Mór')
    street = street.replace('Szabó D.', 'Szabó Dezső')
    street = street.replace('Kossuth F.', 'Kossuth F.')
    street = street.replace('Móricz Zs.', 'Móricz Zsigmond')
    street = street.replace('Hunyadi J ', 'Hunyadi János')
    street = street.replace('Szilágyi E ', 'Szilágyi Erzsébet fasor')
    street = street.replace('Erzsébet Királyné út', 'Erzsébet királyné útja')
    street = street.replace('Mammut', '')
    street = street.replace('Szt. ', 'Szent ')
    street = street.replace(' u.', ' utca')
    street = street.replace(' U.', ' utca')
    street = street.replace('.u.', ' utca')
    street = street.replace(' u ', ' utca')
    street = street.replace(' krt.', ' körút')
    street = street.replace(' Krt.', ' körút')
    street = street.replace(' KRT.', ' körút')
    street = street.replace(' ltp.', ' lakótelep')
    street = street.replace(' Ltp.', ' lakótelep')
    street = street.replace(' LTP.', ' lakótelep')
    street = street.replace(' ltp', ' lakótelep')
    street = street.replace(' sgt.', ' sugárút')
    street = street.replace('^4. sz$', '4. számú főközlekedési')
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
