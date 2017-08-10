#!/usr/bin/env python3
# coding: utf-8

"""MediathekView-Commandline-Downloader

Usage:
  {cmd} list [options] [--sets=<file>] [--count=<results>] [<filter>...]
  {cmd} dump [options] [--sets=<file>] [<filter>...]
  {cmd} download [options] [--sets=<file>] [--low|--high] [<filter>...]
  {cmd} history [options] [--reset|--remove=<hash>]

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
  -d <path>, --dir=<path>               Directory to put the databases in (default is
                                        the current working directory).
  --include-future                      Include shows that have not yet started.

List options:
  -c <results>, --count=<results>       Limit the number of results. [default: 50]

History options:
  --reset                               Reset the list of downloaded shows.
  --remove=<hash>                       Remove a single show from the history.

Download options:
  -h, --high                            Download best available version.
  -l, --low                             Download the smallest available version .
  -t, --target=<path>                   Directory to put the downloaded files in. May contain
                                        the parameters {{dir}} (from the option --dir),
                                        {{filename}} (from server filename) and {{ext}} (file
                                        name extension including the dot), and all fields from
                                        the listing plus {{date}} and {{time}} (the single parts
                                        of {{start}}).
                                        [default: {{dir}}/{{channel}}/{{topic}}/{{start}} {{title}}{{ext}}]
  --mark-only                           Do not download any show, but mark it as downloaded
                                        in the history. This is to initialize a new filter
                                        if upcoming shows are wanted.
  -s <file>, --sets=<file>              A file to load different sets of filters (see below
                                        for details). In the file every different filter set
                                        is expected to be on a new line.

  WARNING: Please be aware that ancient RTMP streams are not supported
           They will not even get listed.

Filters:

  Use filter to select only the shows wanted. Syntax is always <field><operator><pattern>.

  The following operators and fields are available:

   '='  Pattern is a regular expression case insensitive search within the field value.
        Available for the fields 'description', 'start', 'duration', 'age', 'region',
        'size', 'channel', 'topic', 'title', 'hash' and 'url'.

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

Filter sets:

  In commandline with a single run one can only give one set of filters. In most cases
  this means one can only select a single show to list or download with one run.

  For --sets, a file should be given, where every line contains the same filter arguments
  that one would give on the commandline. The lines are filtered one after another and
  then processed together.

  A text file could look for example like this:

    channel=ARD topic='extra 3' title!=spezial duration+20m
    channel=ZDF topic='Die Anstalt' duration+45m
    channel=ZDF topic=heute-show duration+20m

  If additional filters where given through the commandline, all filter sets are extended
  by these filters. Be aware that this is not faster then running all queries separately
  but just more comfortable.

 """

import json
import logging
import lzma
import os
import random
import re
import sys
import time
import shutil
import traceback
import tempfile
import codecs
import xxhash
import urllib.parse
import pickle
import shlex
import fcntl
from contextlib import contextmanager
from itertools import islice
from itertools import chain
from typing import Union, Callable, List, Dict, Any, Generator, Iterable, Tuple
from datetime import datetime, timedelta
from functools import partial

import docopt
import iso8601
import rfc6266
import pytz
import requests
import tzlocal
from pydash import py_
from terminaltables import AsciiTable
from tinydb import TinyDB, Query as TinyQuery
from tinydb_serialization import Serializer as TinySerializer
from tinydb_serialization import SerializationMiddleware as TinySerializationMiddleware
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

DATABASE_CACHE_FILENAME = '.Filmliste-akt.xz.cache'

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


# noinspection PyClassHasNoInit
class DateTimeSerializer(TinySerializer):

    OBJ_CLASS = datetime

    def encode(self, obj):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')

    def decode(self, s):
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')


# noinspection PyClassHasNoInit
class TimedeltaSerializer(TinySerializer):

    OBJ_CLASS = timedelta

    def encode(self, obj):
        return str(obj.total_seconds())

    def decode(self, s):
        return timedelta(seconds=float(s))


class ConfigurationError(Exception):
    pass


@contextmanager
def flocked(fd, timeout=1.0, sleep=0.1):
    """ Contextmanager to lock a file descriptor for exclusive reading/writing. """

    remaining_time = timeout
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                yield
            except IOError:
                if remaining_time or not timeout:
                    time.sleep(sleep)
                else:
                    raise
            else:
                break

    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)


def pickle_cache(cache_file: Union[Callable, str]) -> Callable:
    """ Decorator which will use "cache_file" for caching the results of the decorated function.

    :param cache_file: Either the path to the cache file as string or a callable, that
        called with same (kw)arguments as the decorated function, returns the path to the designated
        cache file as string.
    """

    def decorator(fn):
        # noinspection PyBroadException
        def wrapped(*args, **kwargs):
            _cache_file = cache_file(*args, **kwargs) if callable(cache_file) else cache_file
            try:
                with open(_cache_file, 'rb') as cache_handle:
                    with flocked(cache_handle, timeout=60):
                        logging.debug("Using cached result from %r.", _cache_file)
                        return pickle.load(cache_handle)
            except:
                res = fn(*args, **kwargs)
                with open(_cache_file, 'wb') as cache_handle:
                    with flocked(cache_handle, timeout=60):
                        logging.debug("Saving result to cache %r.", _cache_file)
                        try:
                            pickle.dump(res, cache_handle)
                        except:
                            pass
                return res

        return wrapped

    return decorator


class Database(object):

    def __init__(self, cache_file_path: str) -> None:
        self.cache_file_path = cache_file_path
        self._db = None

    @staticmethod
    def _qualify_url(basis: str, extension: str) -> Union[str, None]:
        if extension:
            if '|' in extension:
                offset, text = extension.split('|', maxsplit=1)
                return basis[:int(offset)] + text
            else:
                return basis + extension
        else:
            return None

    @staticmethod
    def _duration_in_seconds(duration: str) -> int:
        if duration:
            match = re.match(r'(?P<h>\d+):(?P<m>\d+):(?P<s>\d+)', duration)
            if match:
                parts = match.groupdict()
                return int(timedelta(hours=int(parts['h']),
                                     minutes=int(parts['m']),
                                     seconds=int(parts['s'])).total_seconds())
        return 0

    @staticmethod
    def _show_hash(show: Dict) -> str:
        h = xxhash.xxh32()
        h.update(show.get('channel'))
        h.update(show.get('topic'))
        h.update(show.get('title'))
        h.update(str(show.get('size')))
        h.update(str(show.get('start').timestamp()))
        return h.hexdigest()

    @property
    @contextmanager
    def _filmliste(self, retries: int = 5) -> str:
        while retries:
            retries -= 1
            try:
                response = requests.get(random.choice(DATABASE_URLS), stream=True)
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))  # type: ignore
                with tempfile.NamedTemporaryFile(mode='wb') as destination_file:
                    with tqdm(total=total_size,
                              unit='B',
                              unit_scale=True,
                              leave=False,
                              disable=HIDE_PROGRESSBAR,
                              desc='Downloading database') as progress_bar:
                        for data in response.iter_content(32 * 1024):
                            progress_bar.update(len(data))
                            destination_file.write(data)
                    yield destination_file
            except requests.exceptions.HTTPError as e:
                if retries:
                    logging.debug('Database download failed (%d more retries): %s' % (retries, e))
                else:
                    logging.error('Database download failed (no more retries): %s' % e)
                    raise requests.exceptions.HTTPError('retry limit reached, giving up')
                time.sleep(5 - retries)
            else:
                break

    @property  # type: ignore
    def _script_version(self) -> int:
        return os.path.getmtime(__file__)

    @pickle_cache(lambda db: db.cache_file_path)
    def _load(self) -> Dict[str, Union[int, List, Dict[str, Any]]]:

        meta = {}  # type: Dict
        items = []  # type: List
        header = []  # type: List

        logging.debug('Opening the database.')
        with self._filmliste as filmliste:
            with lzma.open(filmliste.name) as fh:

                logging.debug('Loading database items.')
                reader = codecs.getreader("utf-8")
                for p in tqdm(json.load(reader(fh), object_pairs_hook=lambda _pairs: _pairs),
                              unit='shows',
                              leave=False,
                              disable=HIDE_PROGRESSBAR,
                              desc='Reading database items'):

                    if not meta and p[0] == 'Filmliste':
                        meta = {
                            # p[1][0] is local date, p[1][1] is gmt date
                            'date': datetime.strptime(p[1][1], '%d.%m.%Y, %H:%M').replace(tzinfo=pytz.utc),
                            'crawler_version': p[1][2],
                            'crawler_agent': p[1][3],
                            'list_id': p[1][4],
                        }

                    elif p[0] == 'Filmliste':
                        if not header:
                            header = p[1]
                            for i, h in enumerate(header):
                                header[i] = FIELDS.get(h, h)

                    elif p[0] == 'X':
                        show = dict(zip(header, p[1]))
                        if show['start'] and show['url'] and show['size']:
                            item = {
                                'channel': show['channel'] or items[-1].get('channel'),
                                'description': show['description'],
                                'region': show['region'],
                                'size': int(show['size']) if show['size'] else 0,
                                'title': show['title'],
                                'topic': show['topic'] or items[-1].get('topic'),
                                'website': show['website'],
                                'new': show['new'] == 'true',
                                'url_http': show['url'] or None,
                                'url_http_hd': self._qualify_url(show['url'], show['url_hd']),
                                'url_http_small': self._qualify_url(show['url'], show['url_small']),
                                'url_subtitles': show['url_subtitles'],
                                'start': datetime.fromtimestamp(int(show['start']), tz=pytz.utc).astimezone(local_zone),
                                'duration': timedelta(seconds=self._duration_in_seconds(show['duration'])),
                                'age': now - datetime.fromtimestamp(int(show['start']), tz=pytz.utc)
                            }
                            item['hash'] = self._show_hash(item)
                            items.append(item)

        return {
            'version': self._script_version,
            'meta': meta,
            'items': items
        }

    @property
    def db(self):
        if not self._db:
            self._db = self._load()
        return self._db

    def clear(self):
        """ Drop the pickled cache file for this database. """

        self._db = None
        # noinspection PyBroadException
        try:
            os.unlink(self.cache_file_path)
        except:
            pass
        else:
            logging.debug("Dropped cache %r.", self.cache_file_path)

    def clear_if_foreign(self):
        if self.db['version'] != self._script_version:
            logging.debug('Database is for a different script version.')
            self.clear()

    def clear_if_old(self, refresh_after):
        database_age = now - self.db['meta']['date']
        if database_age > timedelta(hours=refresh_after):
            logging.debug('Database age is %s (too old).', database_age)
            self.clear()
        else:
            logging.debug('Database age is %s.', database_age)


def escape_item(obj: Any) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return re.sub(r'(\d+)', r' \1', durationpy.to_str(obj, extended=True)).strip()
    else:
        return str(obj)


def filter_items(items: List[Dict[str, Any]],
                 rules: List[str],
                 include_future: bool=False) -> Generator[Dict[str, Any], None, None]:

    definition = []
    for f in rules:
        match = re.match(r'^(?P<field>\w+)(?P<operator>(?:=|!=|\+|-|\W+))(?P<pattern>.*)$', f)
        if match:

            field, operator, pattern = match.group('field'), \
                                       match.group('operator'), \
                                       match.group('pattern')  # type: str, str, Any

            # replace odd names
            field = {
                'url': 'url_http'
            }.get(field, field)

            if field in ('description', 'region', 'size', 'channel', 'topic', 'title', 'hash', 'url_http'):
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
                if field in ('description', 'duration', 'age', 'region', 'size', 'channel',
                             'topic', 'title', 'hash', 'url_http'):
                    definition.append((field, partial(lambda p, v: bool(re.search(p, v, re.IGNORECASE)), pattern)))
                elif field in ('start', ):
                    definition.append((field, partial(lambda p, v: v == p, pattern)))
                else:
                    raise ConfigurationError('Invalid operator %r for %r.' % (operator, field))
            elif operator == '!=':
                if field in ('description', 'duration', 'age', 'region', 'size', 'channel',
                             'topic', 'title', 'hash', 'url_http'):
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

    logging.debug('Applying filter: %s', ', '.join(rules))
    for row in items:
        if not include_future and row['start'] > now:
            continue
        try:
            if not definition:
                yield row
            else:
                if all(check(row.get(field)) for field, check in definition):
                    yield row
        except (ValueError, TypeError):
            pass


class History(object):

    def __init__(self, cwd):
        self._cwd = cwd

    @property
    @contextmanager
    def db(self) -> TinyDB:
        history_storage = TinySerializationMiddleware()
        history_storage.register_serializer(DateTimeSerializer(), 'Datetime')
        history_storage.register_serializer(TimedeltaSerializer(), 'Timedelta')
        history_file_path = os.path.join(self._cwd, '.history._db')
        try:
            os.makedirs(os.path.dirname(history_file_path))
        except FileExistsError:
            pass
        finally:
            db = TinyDB(history_file_path, default_table='history', storage=history_storage)
            yield db
            db.close()

    @property
    def all(self):
        with self.db as db:
            return db.all()

    def check(self, shows: Iterable[Dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
        row = TinyQuery()
        with self.db as db:
            for item in shows:
                historic_download = db.get(row.hash == item['hash'])
                if historic_download:
                    item['downloaded'] = historic_download['downloaded']
                yield item

    def purge(self):
        with self.db as db:
            return db.purge_tables()

    def remove(self, show_hash):
        row = TinyQuery()
        with self.db as db:
            if db.remove(row.hash == show_hash):
                logging.info('Removed %s from history.', show_hash)
                return True
            else:
                logging.warning('Could not remove %s (not found).', show_hash)
                return False

    def insert(self, show):
        with self.db as db:
            return db.insert(show)


def item_table(shows: Iterable[Dict[str, Any]]) -> str:
    headers = ['hash', 'channel', 'title', 'topic', 'size', 'start', 'duration', 'age', 'region', 'downloaded']
    data = [[escape_item(row.get(h)) for h in headers] for row in shows]
    return AsciiTable([headers] + data).table


def serialize_for_json(obj: Any) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return str(obj)
    else:
        raise TypeError('%r is not JSON serializable' % obj)


def download_files(destination_dir_path: str, target_urls: List[str], title: str) -> Generator[str, None, None]:

    file_sizes = []
    with tqdm(unit='B',
              unit_scale=True,
              leave=False,
              disable=HIDE_PROGRESSBAR,
              desc='Downloading %r' % title) as progress_bar:

        for url in target_urls:

            # determine file size for progressbar
            response = requests.get(url, stream=True)
            file_sizes.append(int(response.headers.get('content-length', 0)))  # type: ignore
            progress_bar.total = sum(file_sizes) / len(file_sizes) * len(target_urls)

            # determine file name and destination
            default_filename = os.path.basename(url)
            file_name = rfc6266.parse_requests_response(response).filename_unsafe or default_filename
            destination_file_path = os.path.join(destination_dir_path, file_name)

            # actual download
            with open(destination_file_path, 'wb') as fh:
                for data in response.iter_content(32 * 1024):
                    progress_bar.update(len(data))
                    fh.write(data)

            yield destination_file_path


def move_finished_download(source_path, cwd, target, show, file_name, file_extension):

    escaped_show_details = {k: str(v).replace(os.path.sep, '_') for k, v in show.items()}
    destination_file_path = target.format(dir=cwd,
                                          filename=file_name,
                                          ext=file_extension,
                                          date=show['start'].date().isoformat(),
                                          time=show['start'].strftime('%H:%M'),
                                          **escaped_show_details)

    try:
        try:
            os.makedirs(os.path.dirname(destination_file_path))
        except FileExistsError:
            pass
        os.rename(source_path, destination_file_path)
    except OSError as e:
        logging.warning('Skipped %r from %s: %s', show['title'], show['start'], str(e))
    else:
        logging.info('Saved %r from %s to %r.', show['title'], show['start'], destination_file_path)


def get_m3u8_segments(base_url: str, hls_file_path: str) -> Generator[Dict[str, Any], None, None]:

    with open(hls_file_path, 'r+') as fh:
        segment = {}  # type: Dict
        for line in fh:
            if not line:
                continue
            elif line.startswith("#EXT-X-STREAM-INF:"):
                # see https://tools.ietf.org/html/draft-pantos-http-live-streaming-16#section-4.3.4.2
                segment = {m.group(1).lower(): m.group(2).strip() for m in re.finditer(r'([A-Z-]+)=([^,]+)', line)}
                for key, value in segment.items():
                    if value[0] in ('"', "'") and value[0] == value[-1]:
                        segment[key] = value[1:-1]
                    else:
                        try:
                            segment[key] = int(value)
                        except ValueError:
                            pass
            elif not line.startswith("#"):
                segment['url'] = urllib.parse.urljoin(base_url, line.strip())
                yield segment
                segment = {}


def download_hls_target(temp_dir_path: str,
                        base_url: str,
                        title: str,
                        quality_preference: Tuple[str, str, str],
                        hls_file_path: str) -> str:

    # get the available video streams ordered by quality
    hls_index_segments = py_ \
        .chain(get_m3u8_segments(base_url, hls_file_path)) \
        .filter(lambda s: 'mp4a' not in s.get('codecs')) \
        .filter(lambda s: s.get('bandwidth')) \
        .sort(key=lambda s: s.get('bandwidth')) \
        .value()

    # select the wanted stream
    if quality_preference[0] == '_hd':
        designated_index_segment = hls_index_segments[-1]
    elif quality_preference[0] == '_small':
        designated_index_segment = hls_index_segments[0]
    else:
        designated_index_segment = hls_index_segments[len(hls_index_segments) // 2]

    designated_index_file = list(download_files(temp_dir_path, [designated_index_segment['url']], title=title))[0]
    logging.debug('Selected HLS bandwidth is %d (available: %s).',
                  designated_index_segment['bandwidth'], ', '.join(str(s['bandwidth']) for s in hls_index_segments))

    # get stream segments
    hls_target_segments = list(get_m3u8_segments(base_url, designated_index_file))
    hls_target_files = download_files(temp_dir_path, list(s['url'] for s in hls_target_segments), title=title)
    logging.debug('%d HLS segments to download.', len(hls_target_segments))

    # download and join the segment files
    fd, temp_file_path = tempfile.mkstemp(dir=temp_dir_path, prefix='.tmp')
    with open(temp_file_path, 'wb') as out_fh:
        for segment_file_path in hls_target_files:

            with open(segment_file_path, 'rb') as in_fh:
                out_fh.write(in_fh.read())

            # delete the segment file immediately to save disk space
            os.unlink(segment_file_path)

    return temp_file_path


def download_show(show: Dict[str, Any], quality: Tuple[str, str, str], cwd: str, target: str):

    temp_path = tempfile.mkdtemp(prefix='.tmp')
    try:

        # show url based on quality preference
        show_url = show["url_http%s" % quality[0]] \
                   or show["url_http%s" % quality[1]] \
                   or show["url_http%s" % quality[2]]

        logging.debug('Downloading %r for %r on %s.', show_url, show['title'], show['start'])
        show_file_path = list(download_files(temp_path, [show_url], title=show['title']))[0]
        show_file_name = os.path.basename(show_file_path)
        if '.' in show_file_name:
            show_file_extension = '.%s' % os.path.basename(show_file_path).split('.')[-1]
            show_file_name = show_file_name[:len(show_file_extension)]
        else:
            show_file_extension = ''

        if show_file_extension in ('.mp4', '.flv'):
            move_finished_download(show_file_path, cwd, target, show, show_file_name, show_file_extension)
        elif show_file_extension == '.m3u8':
            ts_file_path = download_hls_target(temp_path, show_url, show['title'], quality, show_file_path)
            move_finished_download(ts_file_path, cwd, target, show, show_file_name, '.ts')
        else:
            logging.error('File extension %s of %r from %s not supported.',
                          show_file_extension, show['title'], show['start'])

    except (requests.exceptions.RequestException, OSError) as e:
        logging.error('Download of %r from %s: failed: %s', show['title'], show['start'], str(e))
    finally:
        shutil.rmtree(temp_path)


def read_filter_sets(sets_file_path, default_filter):
    if sets_file_path:
        with open(os.path.expanduser(sets_file_path), 'r+') as set_fh:
            for line in set_fh:
                if line.strip():
                    yield default_filter + shlex.split(line)
    else:
        yield default_filter


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

    # rfc6266 logging fix (don't expect an upstream fix for that)
    for logging_handler in rfc6266.LOGGER.handlers:
        rfc6266.LOGGER.removeHandler(logging_handler)

    # mute third party modules
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("rfc6266").setLevel(logging.WARNING)

    # ISO8601 logging
    if arguments['--verbose']:
        log_level = logging.DEBUG
    elif arguments['--quiet']:
        log_level = logging.ERROR
    else:
        log_level = logging.INFO

    logging.basicConfig(
        filename=os.path.expanduser(arguments['--logfile']) if arguments['--logfile'] else None,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level)

    sys.excepthook = lambda _c, _e, _t: logging.critical('%s: %s\n%s', _c, _e, ''.join(traceback.format_tb(_t)))

    # temp file and download config
    cw_dir = os.path.abspath(os.path.expanduser(arguments['--dir']) if arguments['--dir'] else os.getcwd())
    target_dir = os.path.expanduser(arguments['--target'])
    try:
        os.makedirs(cw_dir)
    except FileExistsError:
        pass
    finally:
        tempfile.tempdir = cw_dir

    #  tracking
    history = History(cwd=cw_dir)

    try:
        if arguments['history']:
            if arguments['--reset']:
                history.purge()
            elif arguments['--remove']:
                history.remove(arguments['--remove'])
            else:
                print(item_table(sorted(history.all, key=lambda s: s.get('downloaded'))))

        else:
            filmliste = Database(cache_file_path=os.path.join(cw_dir, DATABASE_CACHE_FILENAME))
            filmliste.clear_if_foreign()
            filmliste.clear_if_old(refresh_after=int(arguments['--refresh-after']))

            limit = int(arguments['--count']) if arguments['list'] else None
            shows = history.check(
                islice(
                    chain(*(filter_items(items=filmliste.db['items'],
                                         rules=filter_set,
                                         include_future=arguments['--include-future'])
                            for filter_set
                            in read_filter_sets(sets_file_path=arguments['--sets'],
                                                default_filter=arguments['<filter>']))),
                    limit or None))

            if arguments['list']:
                print(item_table(shows))

            elif arguments['dump']:
                print(json.dumps(list(shows), default=serialize_for_json, indent=4, sort_keys=True))

            elif arguments['download']:
                for item in shows:
                    if not item.get('downloaded'):
                        if not arguments['--mark-only']:
                            if arguments['--high']:
                                quality_preference = ('_hd', '', '_small')
                            elif arguments['--low']:
                                quality_preference = ('_small', '', '_hd')
                            else:
                                quality_preference = ('', '_hd', '_small')
                            download_show(item, quality_preference, cw_dir, target_dir)
                            item['downloaded'] = now
                            history.insert(item)
                        else:
                            item['downloaded'] = now
                            history.insert(item)
                            logging.info('Marked %r from %s as downloaded.', item['title'], item['start'])
                    else:
                        logging.debug('Skipping %r (already loaded on %s)', item['title'], item['downloaded'])

    except ConfigurationError as e:
        logging.error(str(e))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
