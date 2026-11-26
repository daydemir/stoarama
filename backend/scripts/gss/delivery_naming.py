"""The GSS delivery file names: the one generator and validator (docs/GSS_DELIVERY_NAMING.md).

Deniz: "remember the delivery names system ... we need to stick with that". The
spec is the live catalog's own `naming` (public/catalog.json); NAMING_SPEC is that
text, and catalog_names() refuses a catalog whose spec differs.

  <id>_<Continent>_<Country>_<City>_<Plaza>/<Month>/<Weekday>/
    <id>_<Plaza>_<YYYY>_<Month>_W<1-5>_<Weekday>_hour_<01-12>[_part_NN].mp4

- id is the recording_id; Plaza is the stream's plaza name (its display name when
  the catalog has none); an absent geographic field is Unresolved_Continent,
  Unresolved_Country or Unresolved_City.
- Month and Weekday are English names of the stream-local date; W is the
  calendar-month week (days 1-7 W1, 8-14 W2, 15-21 W3, 22-28 W4, 29-31 W5).
- hour is the scheduled slot ordinal 01-12 (the delivery hour; local 08-19), not
  the clock hour.
- An hour with one current file is plain _hour_XX.mp4. A fragmented hour numbers its
  current files _part_01, _part_02, ... in source order, and a file keeps its
  number while others of its hour are not yet delivered.
- Tokens are sanitized (NFC letters and digits kept, spaces to underscores, other
  punctuation except hyphen and dot removed), and every path is unique under NFC
  case-fold.
"""
import collections
import datetime
import re
import unicodedata

NAMING_SPEC = {
    'week': 'W1–W5: days 1–7, 8–14, 15–21, 22–28, 29–31 of the month',
    'day': 'English weekday, matching the weekday folder',
    'hour': '01–12 are scheduled slot ordinals; local clock range is shown separately',
    'id': 'Stoarama recording_id for every stream, exactly as selected in the site; no plaza_id substitution or padding',
    'parts': 'Single current hour files use plain _hour_XX.mp4; fragmented hours retain _part_01, _part_02, … in source order.',
    'sanitation': 'NFC Unicode letters/digits retained; spaces become underscores; punctuation except hyphen/dot removed; '
                  'traversal, empty and reserved tokens prevented. All paths unique under Unicode NFC case-fold.',
    'missing_metadata': 'Recording ID and verified stream display plaza name retained; absent geographic fields use explicit '
                        'Unresolved_Continent, Unresolved_Country and Unresolved_City labels with a warning',
}
MONTHS = ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
          'November', 'December')
WEEKDAYS = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
GEO = ('continent', 'country', 'city')
RESERVED = re.compile(r'(?i)^(con|prn|aux|nul|com[1-9]|lpt[1-9])$')


class NamingError(ValueError):
    pass


def sanitize(raw):
    out = []
    for ch in unicodedata.normalize('NFC', (raw or '').strip()):
        if ch in '-.':
            out.append(ch)
        elif ch == '_' or ch.isspace():
            out.append('_')
        elif ch.isalnum():
            out.append(ch)
    token = re.sub('_+', '_', ''.join(out)).strip('_.')
    if RESERVED.match(token):
        raise NamingError(f'reserved name token {token!r}')
    return token


def catalog_names(catalog):
    """{recording_id: (folder, stem)} from catalog.json. Fails when the catalog's naming
    spec differs from NAMING_SPEC or a stream it already names would get another folder."""
    spec = {k: catalog.get('naming', {}).get(k) for k in NAMING_SPEC}
    if spec != NAMING_SPEC:
        raise NamingError(f'catalog naming differs from the delivery naming spec: {spec}')
    out = {}
    for c in catalog['streams']:
        rid, m = c['id'], c['metadata']
        plaza = sanitize(m.get('plaza_name') or c['name'])
        if not plaza:
            raise NamingError(f'recording {rid}: no plaza name to name its files')
        folder = '_'.join([str(rid)] + [sanitize(m.get(k)) or f'Unresolved_{k.title()}' for k in GEO] + [plaza])
        if c['folder'] is not None and c['folder'] != folder:
            raise NamingError(f'recording {rid}: catalog folder {c["folder"]!r} differs from its name {folder!r}')
        out[rid] = (folder, f'{rid}_{plaza}')
    return out


def _date(date):
    d = datetime.date.fromisoformat(date)
    return d, MONTHS[d.month - 1], WEEKDAYS[d.weekday()], (d.day - 1) // 7 + 1


def hour_stem(names, rid, date, delivery_hour):
    if not 1 <= delivery_hour <= 12:
        raise NamingError(f'delivery hour {delivery_hour} is not a slot 01-12')
    folder, stem = names[rid]
    d, month, weekday, week = _date(date)
    return f'{folder}/{month}/{weekday}/{stem}_{d.year}_{month}_W{week}_{weekday}_hour_{delivery_hour:02d}'


def delivery_path(names, rid, date, delivery_hour, ordinal, count):
    """The name of the ordinal-th (1-based, source order) of an hour's count current files."""
    if not 1 <= ordinal <= count <= 99:
        raise NamingError(f'part {ordinal} of {count}')
    return hour_stem(names, rid, date, delivery_hour) + (f'_part_{ordinal:02d}' if count > 1 else '') + '.mp4'


def check_paths(names, files):
    """Problems with served files [(recording_id, local_date, path)], in source order per hour:
    every path is a delivery name for its stream and date, an hour is either one plain file
    or numbered parts in increasing order, and paths are unique under NFC case-fold."""
    problems, hours = [], collections.defaultdict(list)
    for rid, date, path in files:
        for dh in range(1, 13):
            base = hour_stem(names, rid, date, dh)
            if path == base + '.mp4':
                hours[(rid, date, dh)].append((path, 0))
                break
            m = re.fullmatch(re.escape(base) + r'_part_([0-9]{2})\.mp4', path)
            if m and int(m.group(1)) >= 1:
                hours[(rid, date, dh)].append((path, int(m.group(1))))
                break
        else:
            problems.append(f'{path}: not a delivery name of recording {rid} on {date}')
    for slot, parts in hours.items():
        ordinals = [n for _, n in parts]
        if (0 in ordinals and len(parts) > 1) or ordinals != sorted(set(ordinals)):
            problems.append(f'recording {slot[0]} {slot[1]} hour {slot[2]:02d}: parts {ordinals} are not one plain file or increasing numbered parts')
    folded = collections.Counter(unicodedata.normalize('NFC', p).casefold() for _, _, p in files)
    problems += [f'{p}: not unique under NFC case-fold' for p, n in folded.items() if n > 1]
    return problems
