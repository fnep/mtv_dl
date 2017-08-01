#!/usr/bin/env python3
# coding: utf-8

"""MediathekView-dl

Usage:
  {cmd} list [options] [-c =<results>] [<filter>...]
  {cmd} dump [options] [<filter>...]
  {cmd} history [--reset]
  {cmd} download [options] [--small|--high] [<filter>...]

Commands:
  list                                  Show the list of query results as ascii table.
  dump                                  Show the list of query results as json list.
  history                               Show the list of downloaded shows.
  download                              Download shows in the list of query results.

Options:
  -v, --verbose                         Show more details.
  -q, --quiet                           Hide everything not really needed.
  -l <path>, --logfile=<path>           Log messages to a file instead of stdout.
  -r <hours>, --refresh-after=<hours>   Update database if it is older then the given
                                        number of hours. [default: 3]
  -w <path>, --cwd=<path>               Directory to put the databases in (default is
                                        the current working directory).

List options:
  -c <results>, --count=<results>       Limit the number of results. [default: 50]

History options:
  --reset                               Reset the list of downloaded shows.

Download options:
  -h, --high                            Download the high definition version (if available).
  -s, --small                           Download the small version (if available).
  -t, --target=<path>                   Directory to put the downloaded files in. May contain
                                        the parameters {{cwd}} (from the option --cwd),
                                        {{filename}} (server filename), and all fields from the
                                        listing. [default: {{cwd}}/{{channel}}/{{filename}}]

  WARNING: Please be aware that ancient RTMP streams are not supported
           They will not even get listed.

Filters:

  Use filter to select only the shows wanted. Syntax is always <field><operator><pattern>.

  The following operators and fields are available:

   '='  Pattern is a regular expression case insensitive search within the field value.
        Available for the fields 'description', 'start', 'duration', 'age', 'region',
        'size', 'channel', 'topic' and 'title'.

   '!=' Pattern is a regular expression that must not appear in a case insensitive search
        within the field value. Available on the same fields as for the '=' operator.

   '+'  Pattern must be greater then the field value. Available for the fields 'duration',
        'age', 'start' and 'size'.

   '-'  Pattern must be less then the field value. Available for the same fields as for
        the '+' operator.

  Pattern should be given in the same format as shown in the list command. Times (for
  'start'), time deltas (for 'duration', 'age') and numbers ('size') are parsed and
  smart compared.

  Examples:
    - topic='extra 3'                   (topic contains 'extra 3')
    - title!=spezial                    (title contains 'spezial')
    - channel=ARD                       (channel contains ARD)
    - age-1mm                           (age is older then 1 month)
    - duration+20m                      (duration longer then 20 min)
    - start+2017-07-01                  (show started after 2017-07-01)
    - start-2017-07-05T23:00:00+02:00   (show started before 2017-07-05, 23:00 CEST)

  As many filters as needed may be given as separated arguments (separated  with space).
  For a show to get considered, _all_ given filter criteria must met.

 """

import json
import logging
import lzma
import os
import random
import re
import sys
import time
import traceback
import itertools
import codecs
from datetime import datetime, timedelta
from functools import lru_cache
from functools import partial

import docopt
import iso8601
import pytz
import requests
import tzlocal
from terminaltables import AsciiTable
from tinydb import TinyDB, Query
from tqdm import tqdm

import durationpy

HIDE_PROGRESSBAR = True

FIELDS = {
    'Beschreibung': 'description',
    'Datum': 'date',
    'DatumL': 'start',
    'Dauer': 'duration',
    'Geo': 'region',
    'Größe [MB]': 'size',
    'Sender': 'channel',
    'Thema': 'topic',
    'Titel': 'title',
    'Url': 'url',
    'Url HD': 'url_hd', 'Url History': 'url_history',
    'Url Klein': 'url_small',
    'Url RTMP': 'url_rtmp',
    'Url RTMP HD': 'url_rtmp_hd',
    'Url RTMP Klein': 'url_rtmp_small',
    'Url Untertitel': 'url_subtitles',
    'Website': 'website',
    'Zeit': 'time',
    'neu': 'new'
}

DATABASE_FILENAME = 'Filmliste-akt.xz'

DATABASE_URLS = [
    "http://verteiler1.mediathekview.de/Filmliste-akt.xz",
    "http://verteiler2.mediathekview.de/Filmliste-akt.xz",
    "http://verteiler3.mediathekview.de/Filmliste-akt.xz"
    "http://verteiler4.mediathekview.de/Filmliste-akt.xz",
    "http://verteiler5.mediathekview.de/Filmliste-akt.xz",
    "http://verteiler6.mediathekview.de/Filmliste-akt.xz",
    "http://download10.onlinetvrecorder.com/mediathekview/Filmliste-akt.xz",
]

local_zone = tzlocal.get_localzone()
now = datetime.now(tz=pytz.utc).replace(second=0, microsecond=0)
cwd = os.getcwd()
history_db = TinyDB(os.path.join(cwd, 'history.db'), default_table='history')


class ConfigurationError(Exception):
    pass


def qualify_url(basis, extension):
    if extension:
        if '|' in extension:
            offset, text = extension.split('|', maxsplit=1)
            return basis[:int(offset)] + text
        else:
            return basis + extension


def duration_in_seconds(duration):
    if duration:
        match = re.match(r'(?P<h>\d+):(?P<m>\d+):(?P<s>\d+)', duration)
        if match:
            parts = match.groupdict()
            return int(timedelta(hours=int(parts['h']),
                                 minutes=int(parts['m']),
                                 seconds=int(parts['s'])).total_seconds())
    else:
        return 0


class Database(object):

    def __init__(self, path):
        self.path = path

    @property
    @lru_cache(maxsize=None)
    def _pairs(self):
        logging.debug('Opening the database.')
        reader = codecs.getreader("utf-8")
        return json.load(reader(lzma.open(self.path, 'rb')), object_pairs_hook=lambda _pairs: _pairs)

    @property
    @lru_cache(maxsize=None)
    def meta(self):
        for p in self._pairs:
            if p[0] == 'Filmliste':
                return {
                    # p[1][0] is local date, p[1][1] is gmt date
                    'date': datetime.strptime(p[1][1], '%d.%m.%Y, %H:%M').replace(tzinfo=pytz.utc),
                    'crawler_version': p[1][2],
                    'crawler_agent': p[1][3],
                    'list_id': p[1][4],
                }

    @property
    def items(self):

        header = []
        last_item = {}

        logging.debug('Loading database items.')
        for p in tqdm(self._pairs[1:],
                      unit='shows',
                      leave=False,
                      disable=HIDE_PROGRESSBAR,
                      desc='reading database items'):

            if p[0] == 'Filmliste':
                if not header:
                    header = p[1]
                    for i, h in enumerate(header):
                        header[i] = FIELDS.get(h, h)

            elif p[0] == 'X':
                show = dict(zip(header, p[1]))
                if show['start'] and show['url']:  # skip live streams (no start time) and RTMP-only shows
                    item = {
                        'channel': show['channel'] or last_item.get('channel'),
                        'description': show['description'],
                        'region': show['region'],
                        'size': int(show['size']) if show['size'] else 0,
                        'title': show['title'],
                        'topic': show['topic'] or last_item.get('topic'),
                        'website': show['website'],
                        'new': show['new'] == 'true',
                        'url_http': show['url'] or None,
                        'url_http_hd': qualify_url(show['url'], show['url_hd']),
                        'url_http_small': qualify_url(show['url'], show['url_small']),
                        'url_subtitles': show['url_subtitles'],
                        'start': datetime.fromtimestamp(int(show['start']), tz=pytz.utc).astimezone(local_zone),
                        'duration': timedelta(seconds=duration_in_seconds(show['duration'])),
                        'age': now - datetime.fromtimestamp(int(show['start']), tz=pytz.utc)
                    }
                    last_item = item
                    yield item


def download_database(destination_path, retries=5):
    while retries:
        retries -= 1
        try:
            response = requests.get(random.choice(DATABASE_URLS), stream=True)
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            chunk_size = 32 * 1024
            with open(destination_path, 'wb') as f:
                with tqdm(
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        leave=False,
                        disable=HIDE_PROGRESSBAR,
                        desc='downloading database') as progress_bar:
                    for data in response.iter_content(chunk_size):
                        progress_bar.update(len(data))
                        f.write(data)
            return os.stat(destination_path).st_size
        except requests.exceptions.HTTPError as e:
            if retries:
                logging.debug('Database download failed (%d more retries): %s' % (retries, e))
            else:
                logging.error('Database download failed (no more retries): %s' % e)
            time.sleep(5-retries)
    raise requests.exceptions.HTTPError('retry limit reached, giving up')


def load_database(refresh_after):
    database_path = os.path.join(cwd, DATABASE_FILENAME)
    try:
        db = Database(database_path)
        database_age = now - db.meta['date']
        logging.debug('Database age is %s.', database_age)
        if database_age < timedelta(hours=refresh_after):
            return db

    except OSError as e:
        logging.debug('Failed to open database: %s', e)

    download_database(database_path)
    return Database(database_path)


def escape_item(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return re.sub(r'(\d+)', r' \1', durationpy.to_str(obj, extended=True)).strip()
    else:
        return str(obj)


def filter_items(items, rules, limit):

    definition = []
    for f in rules:
        match = re.match(r'^(?P<field>\w+)(?P<operator>(?:=|!=|\+|-|\W+))(?P<pattern>.*)$', f)
        if match:

            field, operator, pattern = match.group('field'), match.group('operator'), match.group('pattern')

            if field in ('description', 'region', 'size', 'channel', 'topic', 'title'):
                pattern = str(pattern)
            elif field in ('duration', 'age'):
                pattern = durationpy.from_str(pattern)
            elif field in ('start', ):
                pattern = iso8601.parse_date(pattern)
            elif field in ('size', ):
                pattern = int(pattern)
            else:
                raise ConfigurationError('Invalid filter field: %r' % field)

            if operator == '=':
                if field in ('description', 'duration', 'age', 'region', 'size', 'channel', 'topic', 'title'):
                    definition.append((field, partial(lambda p, v: bool(re.search(p, v, re.IGNORECASE)), pattern)))
                elif field in ('start', ):
                    definition.append((field, partial(lambda p, v: v == p, pattern)))
                else:
                    raise ConfigurationError('Invalid operator %r for %r.' % (operator, field))
            elif operator == '!=':
                if field in ('description', 'duration', 'age', 'region', 'size', 'channel', 'topic', 'title'):
                    definition.append((field, partial(lambda p, v: not bool(re.search(p, v, re.IGNORECASE)), pattern)))
                elif field in ('start', ):
                    definition.append((field, partial(lambda p, v: v != p, pattern)))
                else:
                    raise ConfigurationError('Invalid operator %r for %r.' % (operator, field))
            elif operator == '-':
                if field not in ('duration', 'age', 'size', 'start'):
                    raise ConfigurationError('Invalid operator %r for %r.' % (operator, field))
                definition.append((field, partial(lambda p, v: v <= p, pattern)))
            elif operator == '+':
                if field not in ('duration', 'age', 'size', 'start'):
                    raise ConfigurationError('Invalid operator %r for %r.' % (operator, field))
                definition.append((field, partial(lambda p, v: v >= p, pattern)))
            else:
                raise ConfigurationError('Invalid operator: %r' % operator)

        else:
            raise ConfigurationError('Invalid filter definition. '
                                     'Property and filter rule expected separated by an operator.')

    count = 0
    for row in items:
        if limit and count > limit:
            items.close()
        else:
            try:
                if not definition:
                    count += 1
                    yield row
                else:
                    if all(check(row.get(field)) for field, check in definition):
                        count += 1
                        yield row
            except (ValueError, TypeError):
                pass


def item_table(items):
    headers = ['channel', 'title', 'topic', 'size', 'start', 'duration', 'age', 'region']
    data = [[escape_item(row.get(h)) for h in headers] for row in items]
    return AsciiTable([headers] + data).table


def serialize_for_json(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return str(obj)
    else:
        raise TypeError('%r is not JSON serializable' % obj)


def main():
    arguments = docopt.docopt(__doc__.format(cmd=os.path.basename(__file__)))

    # progressbar handling
    global HIDE_PROGRESSBAR
    HIDE_PROGRESSBAR = bool(arguments['--logfile']) or arguments['--quiet']

    # broken console encoding handling
    if sys.stdout.encoding != 'UTF-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    if sys.stderr.encoding != 'UTF-8':
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

    # ISO8601 logging
    if arguments['--verbose']:
        log_level = logging.DEBUG
    elif arguments['--quiet']:
        log_level = logging.ERROR
    else:
        log_level = logging.INFO

    logging.basicConfig(
        filename=arguments['--logfile'],
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level)

    sys.excepthook = lambda c, e, t: logging.critical('%s: %s\n%s', c, e, ''.join(traceback.format_tb(t)))

    try:
        db = load_database(refresh_after=int(arguments['--refresh-after']))

        item_limit = int(arguments['--count']) if arguments['list'] else None
        items = filter_items(items=db.items, rules=arguments['<filter>'], limit=item_limit)

        if arguments['list']:
            print(item_table(items))
        elif arguments['dump']:
            print(json.dumps(list(items), default=serialize_for_json, indent=4, sort_keys=True))
        elif arguments['history']:
            raise NotImplemented()
        elif arguments['download']:
            raise NotImplemented()
    except ConfigurationError as e:
        logging.error(str(e))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
