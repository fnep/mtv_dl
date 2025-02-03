#!/usr/bin/env python3

import hashlib
import http.client
import json
import logging
import lzma
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import contextmanager
from contextlib import suppress
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from importlib.metadata import version
from io import BytesIO
from itertools import chain
from os import chdir
from pathlib import Path
from ssl import create_default_context
from tempfile import NamedTemporaryFile
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import TypedDict
from xml.etree import ElementTree as Et

import certifi
import durationpy
import ijson
import iso8601
import typer
from bs4 import BeautifulSoup
from rich import box
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import TextColumn
from rich.progress import TimeRemainingColumn
from rich.table import Table
from typer_config import use_yaml_config

CHUNK_SIZE = 128 * 1024

if (default_config_file := Path("~/.mtv_dl.yml").expanduser()).exists():
    CONFIG_FILE: Path | None = default_config_file
else:
    CONFIG_FILE = None
HISTORY_DATABASE_FILE = ".History.sqlite"
FILMLISTE_DATABASE_FILE = f".Filmliste.{version('mtv_dl')}.sqlite"

# regex to find characters not allowed in file names
INVALID_FILENAME_CHARACTERS = re.compile("[{}]".format(re.escape('<>:"/\\|?*' + "".join(chr(i) for i in range(32)))))

# see https://res.mediathekview.de/akt.xml
# and https://forum.mediathekview.de/topic/3508/aktuelle-verteiler-und-filmlisten-server
FILMLISTE_URL = "https://liste.mediathekview.de/Filmliste-akt.xz"

# global state
HIDE_PROGRESSBAR = True
SSL_CONTEXT = create_default_context()
SHOWLIST: "Database"

logger = logging.getLogger("mtv_dl")
utc_zone = timezone.utc
now = datetime.now(tz=utc_zone).replace(second=0, microsecond=0)


# add timedelta database type
sqlite3.register_adapter(timedelta, lambda v: v.total_seconds())
sqlite3.register_converter("timedelta", lambda v: timedelta(seconds=int(v)))

# console handler for tables and progress bars
console = Console()


@contextmanager
def progress_bar() -> Iterator[Progress]:
    progress_console = console
    if HIDE_PROGRESSBAR:
        progress_console = Console(file=open(os.devnull, "w"))  # noqa
    with Progress(
        TextColumn("[bold blue]{task.description}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        TimeRemainingColumn(),
        refresh_per_second=4,
        console=progress_console,
    ) as progress:
        yield progress


class SqlRegexFunction:
    """A simple SQL function to use regular expressions in SQL statements.

    This is a workaround for the missing REGEXP operator in sqlite3.
    It will deduplicate the compiled regular expressions to avoid
    recompiling the same expression over and over again. Also, it will
    log warnings if the regular expression is invalid only once.
    """

    last_error: str = ""
    last_expression: str = ""
    pattern: "re.Pattern[str]" = re.compile("")

    def __call__(self, expr: str, item: str) -> bool:
        try:
            if self.last_expression != expr:
                self.last_expression = expr
                self.pattern = re.compile(expr, re.IGNORECASE)
        except re.error as e:
            if self.last_error != str(e):
                self.last_error = str(e)
                logger.warning("Invalid regular expression %r (using string match instead): %s", expr, e)
            self.pattern = re.compile(re.escape(expr), re.IGNORECASE)
        return self.pattern.search(item) is not None


class ConfigurationError(Exception):
    pass


class RetryLimitExceededError(Exception):
    pass


def serialize_for_json(obj: Any) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return str(obj)
    else:
        raise TypeError(f"{obj!r} is not JSON serializable")


def escape_path(s: str) -> str:
    return INVALID_FILENAME_CHARACTERS.sub("_", s)


class Database:
    # noinspection SpellCheckingInspection
    TRANSLATION: ClassVar = {
        "Beschreibung": "description",
        "Datum": "date",
        "DatumL": "start",
        "Dauer": "duration",
        "Geo": "region",
        "Größe [MB]": "size",
        "Sender": "channel",
        "Thema": "topic",
        "Titel": "title",
        "Url": "url",
        "Url HD": "url_hd",
        "Url History": "url_history",
        "Url Klein": "url_small",
        "Url RTMP": "url_rtmp",
        "Url RTMP HD": "url_rtmp_hd",
        "Url RTMP Klein": "url_rtmp_small",
        "Url Untertitel": "url_subtitles",
        "Website": "website",
        "Zeit": "time",
        "neu": "new",
    }

    class Item(TypedDict):
        hash: str
        channel: str
        description: str
        region: str
        size: int
        title: str
        topic: str
        website: str
        new: bool
        url_http: str | None
        url_http_hd: str | None
        url_http_small: str | None
        url_subtitles: str
        start: datetime
        duration: timedelta
        age: timedelta
        downloaded: datetime | None
        season: int | None
        episode: int | None

    def database_file(self, schema: str = "main") -> Path:
        cursor = self.connection.cursor()
        database_index = {db[1]: db[2] for db in cursor.execute("PRAGMA database_list")}
        if database_index.get(schema):
            return Path(database_index[schema])
        else:
            raise ValueError(f"Database file for {schema!r} not found.")

    @property
    def filmliste_version(self) -> int:
        cursor = self.connection.cursor()
        return int(cursor.execute("PRAGMA main.user_version;").fetchone()[0])

    def initialize_filmliste(self) -> None:
        logger.debug("Initializing Filmliste database in %r.", self.database_file("main"))
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                CREATE TABlE main.show (
                    hash TEXT,
                    channel TEXT,
                    description TEXT,
                    region TEXT,
                    size INTEGER,
                    title TEXT,
                    topic TEXT,
                    website TEXT,
                    new BOOLEAN,
                    url_http TEXT,
                    url_http_hd TEXT,
                    url_http_small TEXT,
                    url_subtitles TEXT,
                    start TIMESTAMP,
                    duration TIMEDELTA,
                    age TIMEDELTA,
                    season INTEGER,
                    episode INTEGER
                );
                """
            )
        except sqlite3.OperationalError:
            cursor.execute("DELETE FROM main.show")

        self.connection.commit()

    def update_filmliste(self) -> None:
        logger.debug("Updating Filmliste database in %r.", self.database_file("main"))
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM main.show")
        cursor.executemany(
            """
            INSERT INTO main.show
            VALUES (
                :hash,
                :channel,
                :description,
                :region,
                :size,
                :title,
                :topic,
                :website,
                :new,
                :url_http,
                :url_http_hd,
                :url_http_small,
                :url_subtitles,
                :start,
                :duration,
                :age,
                :season,
                :episode
            )
            """,
            self._get_shows(),
        )
        cursor.execute(f"PRAGMA user_version={int(now.timestamp())}")
        self.connection.commit()

    def update_if_old(self) -> None:
        database_age = now - datetime.fromtimestamp(self.filmliste_version, tz=utc_zone)
        if database_age > self.filmliste_refresh_after:
            logger.debug("Database age is %s (too old).", database_age)
            self.update_filmliste()
        else:
            logger.debug("Database age is %s.", database_age)

    @property
    def history_version(self) -> int:
        cursor = self.connection.cursor()
        return int(cursor.execute("PRAGMA history.user_version;").fetchone()[0])

    def initialize_history(self) -> None:
        if self.history_version == 0:
            logger.info("Initializing History database in %r.", self.database_file("main"))
            cursor = self.connection.cursor()
            cursor.execute(
                """
                CREATE TABlE history.downloaded (
                    hash TEXT,
                    channel TEXT,
                    description TEXT,
                    region TEXT,
                    size INTEGER,
                    title TEXT,
                    topic TEXT,
                    website TEXT,
                    start TIMESTAMP,
                    duration TIMEDELTA,
                    downloaded TIMESTAMP,
                    season INTEGER,
                    episode INTEGER,
                    UNIQUE (hash)
                );
                """
            )
            cursor.execute("PRAGMA history.user_version=2")
        elif self.history_version == 1:
            logger.info("Upgrading history database schema, adding columns for season and episode")
            # manually control transactions to make sure this schema upgrade is atomic
            old_isolation_level = self.connection.isolation_level
            self.connection.isolation_level = None
            cursor = self.connection.cursor()
            cursor.execute("BEGIN")
            cursor.execute("ALTER TABLE history.downloaded ADD COLUMN season INTEGER")
            cursor.execute("ALTER TABLE history.downloaded ADD COLUMN episode INTEGER")
            cursor.execute("PRAGMA history.user_version=2")
            cursor.execute("COMMIT")
            self.connection.isolation_level = old_isolation_level

    def __init__(self, filmliste: Path, history: Path, filmliste_refresh_after: timedelta = timedelta(hours=3)) -> None:
        logger.debug("Opening Filmliste database %r.", filmliste)
        self.connection = sqlite3.connect(
            filmliste.absolute().as_posix(),
            detect_types=sqlite3.PARSE_DECLTYPES,
            timeout=10,
        )
        logger.debug("Opening History database %r.", history)
        self.connection.cursor().execute("ATTACH ? AS history", (history.as_posix(),))

        self.connection.row_factory = sqlite3.Row
        self.connection.create_function("REGEXP", 2, SqlRegexFunction())
        self.filmliste_refresh_after = filmliste_refresh_after
        if self.filmliste_version == 0:
            self.initialize_filmliste()
        if self.history_version != 2:
            self.initialize_history()

    @staticmethod
    def _qualify_url(basis: str, extension: str) -> str | None:
        if extension:
            if "|" in extension:
                offset, text = extension.split("|", maxsplit=1)
                return basis[: int(offset)] + text
            else:
                return basis + extension
        else:
            return None

    @staticmethod
    def _duration_in_seconds(duration: str) -> int:
        if duration:
            match = re.match(r"(?P<h>\d+):(?P<m>\d+):(?P<s>\d+)", duration)
            if match:
                parts = match.groupdict()
                return int(
                    timedelta(
                        hours=int(parts["h"]),
                        minutes=int(parts["m"]),
                        seconds=int(parts["s"]),
                    ).total_seconds()
                )
        return 0

    @staticmethod
    def _show_hash(channel: str, topic: str, title: str, size: int, start: datetime) -> str:
        h = hashlib.sha1()
        h.update(channel.encode())
        h.update(topic.encode())
        h.update(title.encode())
        h.update(str(size).encode())
        with suppress(OSError, OverflowError):
            # This can happen on some platforms. In this case simply ignore the timestamp for the hash.
            h.update(str(start.timestamp()).encode())

        return h.hexdigest()

    @contextmanager
    def _showlist(self, retries: int = 3) -> Iterator[BytesIO]:
        while retries:
            retries -= 1
            try:
                logger.debug("Opening database from %r.", FILMLISTE_URL)
                response: http.client.HTTPResponse = urllib.request.urlopen(
                    FILMLISTE_URL,
                    timeout=9,
                    context=SSL_CONTEXT,
                )
                total_size = int(response.getheader("content-length") or 0)
                with BytesIO() as buffer:
                    with progress_bar() as progress:
                        bar_id = progress.add_task(total=total_size, description="Downloading database")
                        while True:
                            data = response.read(CHUNK_SIZE)
                            if not data:
                                break
                            else:
                                progress.update(bar_id, advance=len(data))
                                buffer.write(data)
                    buffer.seek(0)
                    yield buffer
            except urllib.error.HTTPError as e:
                if retries:
                    logger.debug("Database download failed (%d more retries): %s" % (retries, e))
                else:
                    logger.error(f"Database download failed (no more retries): {e}")
                    raise RetryLimitExceededError("retry limit reached, giving up")
                time.sleep(10)
            else:
                break

    def _get_shows(self) -> Iterable["Database.Item"]:
        meta: dict[str, Any] = {}
        header: list[str] = []
        channel, topic, region = "", "", ""
        with self._showlist() as showlist_archive, lzma.open(showlist_archive, "rb") as fh:
            logger.debug("Loading database items.")

            # this will loop one time over the whole json for the sole purpose of counting
            # items to be able to render the progressbar below
            items_count = 0
            if not HIDE_PROGRESSBAR:
                items_count = sum(1 for _ in ijson.kvitems(fh, ""))
                fh.seek(0)

            with progress_bar() as progress:
                bar_id = progress.add_task(total=items_count, description="Reading database items")
                for p in ijson.kvitems(fh, ""):
                    progress.update(bar_id, advance=1)
                    if not meta and p[0] == "Filmliste":
                        meta = {
                            # p[1][0] is local date, p[1][1] is gmt date
                            "date": datetime.strptime(p[1][1], "%d.%m.%Y, %H:%M").replace(tzinfo=utc_zone),
                            "crawler_version": p[1][2],
                            "crawler_agent": p[1][3],
                            "list_id": p[1][4],
                        }

                    elif p[0] == "Filmliste":
                        if not header:
                            header = p[1]
                            for i, h in enumerate(header):
                                header[i] = self.TRANSLATION.get(h, h)

                    elif p[0] == "X":
                        show = dict(zip(header, p[1]))
                        channel = show.get("channel") or channel
                        topic = show.get("topic") or topic
                        region = show.get("region") or region
                        if show["start"] and show["url"]:
                            title = show["title"]
                            size = int(show["size"]) if show["size"] else 0

                            # this should work on all platforms.
                            # See https://github.com/fnep/mtv_dl/issues/42 or https://bugs.python.org/issue36439
                            start = datetime.fromtimestamp(0, tz=utc_zone) + timedelta(seconds=int(show["start"]))

                            duration = timedelta(seconds=self._duration_in_seconds(show["duration"]))
                            season, episode = _guess_series_details(title)
                            yield {
                                "hash": self._show_hash(channel, topic, title, size, start.replace(tzinfo=None)),
                                "channel": channel,
                                "description": show["description"],
                                "region": region,
                                "size": size,
                                "title": title,
                                "topic": topic,
                                "website": show["website"],
                                "new": show["new"] == "true",
                                "url_http": str(show["url"]) or None,
                                "url_http_hd": self._qualify_url(show["url"], show["url_hd"]),
                                "url_http_small": self._qualify_url(show["url"], show["url_small"]),
                                "url_subtitles": show["url_subtitles"],
                                "start": start.replace(tzinfo=None),
                                "duration": duration,
                                "age": now - start,
                                "season": season,
                                "episode": episode,
                                "downloaded": None,
                            }

    def add_to_downloaded(self, show: "Database.Item") -> None:
        cursor = self.connection.cursor()
        with suppress(sqlite3.IntegrityError):
            cursor.execute(
                """
                INSERT INTO history.downloaded
                VALUES(
                    :hash,
                    :channel,
                    :description,
                    :region,
                    :size,
                    :title,
                    :topic,
                    :website,
                    :start,
                    :duration,
                    CURRENT_TIMESTAMP,
                    :season,
                    :episode
                )
                """,
                show,
            )

        self.connection.commit()

    def purge_downloaded(self) -> None:
        cursor = self.connection.cursor()
        # noinspection SqlWithoutWhere
        cursor.execute("DELETE FROM history.downloaded")
        self.connection.commit()

    def remove_from_downloaded(self, show_hash: str) -> bool:
        if not len(show_hash) >= 10:
            logger.warning("Show hash to ambiguous %s.", show_hash)
            return False

        cursor = self.connection.cursor()
        cursor.execute("SELECT hash FROM history.downloaded WHERE hash LIKE ?", (show_hash + "%",))
        found_shows = [r[0] for r in cursor.fetchall()]
        if not found_shows:
            logger.warning("Could not remove %s (not found).", show_hash)
            return False
        elif len(found_shows) > 1:
            logger.warning("Could not remove %s (to ambiguous).", show_hash)
            return False
        else:
            cursor.execute("DELETE FROM history.downloaded WHERE hash=?", (found_shows[0],))
            self.connection.commit()
            logger.info("Removed %s from history.", show_hash)
            return True

    @staticmethod
    def read_filter_sets(sets_file_path: Path | None, default_filter: list[str]) -> Iterator[list[str]]:
        if sets_file_path:
            with sets_file_path.expanduser().open("r+", encoding="utf-8") as set_fh:
                for line in set_fh:
                    if line.strip() and not re.match(r"^\s*#", line):
                        yield default_filter + shlex.split(line)
        else:
            yield default_filter

    def filtered(
        self,
        rules: list[str],
        include_future: bool = False,
        limit: int | None = None,
    ) -> Iterator["Database.Item"]:
        where = []
        arguments: list[Any] = []
        if rules:
            logger.debug("Applying filter: %s (limit: %s)", ", ".join(rules), limit)

            for f in rules:
                match = re.match(r"^(?P<field>\w+)(?P<operator>(?:=|!=|\+|-|\W+))(?P<pattern>.*)$", f)
                if match:
                    field, operator, pattern = (
                        match.group("field"),
                        match.group("operator"),
                        match.group("pattern"),
                    )  # type: str, str, Any

                    # replace odd names
                    field = {"url": "url_http"}.get(field, field)

                    if field not in (
                        "description",
                        "region",
                        "size",
                        "channel",
                        "topic",
                        "title",
                        "hash",
                        "url_http",
                        "duration",
                        "age",
                        "start",
                        "dow",
                        "hour",
                        "minute",
                        "season",
                        "episode",
                    ):
                        raise ConfigurationError(f"Invalid field {field!r}.")

                    if operator == "=":
                        if field in ("description", "region", "size", "channel", "topic", "title", "hash", "url_http"):
                            where.append(f"show.{field} REGEXP ?")
                            arguments.append(str(pattern))
                        elif field in ("duration", "age"):
                            where.append(f"show.{field}=?")
                            arguments.append(durationpy.from_str(pattern).total_seconds())
                        elif field in ("start",):
                            where.append(f"show.{field}=?")
                            arguments.append(iso8601.parse_date(pattern).isoformat())
                        elif field in ("dow"):
                            where.append("CAST(strftime('%w', show.start) AS INTEGER)=?")
                            arguments.append(int(pattern))
                        elif field in ("hour"):
                            where.append("CAST(strftime('%H', datetime(show.start, 'localtime')) AS INTEGER)=?")
                            arguments.append(int(pattern))
                        elif field in ("minute"):
                            where.append("CAST(strftime('%M', show.start) AS INTEGER)=?")
                            arguments.append(int(pattern))
                        elif field in ("season", "episode"):
                            where.append(f"show.{field}=?")
                            arguments.append(int(pattern))
                        else:
                            raise ConfigurationError(f"Invalid operator {operator!r} for {field!r}.")

                    elif operator == "!=":
                        if field in ("description", "region", "size", "channel", "topic", "title", "hash", "url_http"):
                            where.append(f"show.{field} NOT REGEXP ?")
                            arguments.append(str(pattern))
                        elif field in ("duration", "age"):
                            where.append(f"show.{field}!=?")
                            arguments.append(durationpy.from_str(pattern).total_seconds())
                        elif field in ("start",):
                            where.append(f"show.{field}!=?")
                            arguments.append(iso8601.parse_date(pattern).isoformat())
                        elif field in ("dow"):
                            where.append("CAST(strftime('%w', show.start) AS INTEGER)!=?")
                            arguments.append(int(pattern))
                        elif field in ("hour"):
                            where.append("CAST(strftime('%H', datetime(show.start, 'localtime')) AS INTEGER)!=?")
                            arguments.append(int(pattern))
                        elif field in ("minute"):
                            where.append("CAST(strftime('%M', show.start) AS INTEGER)!=?")
                            arguments.append(int(pattern))
                        elif field in ("season", "episode"):
                            where.append(f"show.{field}!=?")
                            arguments.append(int(pattern))
                        else:
                            raise ConfigurationError(f"Invalid operator {operator!r} for {field!r}.")

                    elif operator == "-":
                        if field in ("duration", "age"):
                            where.append(f"show.{field}<=?")
                            arguments.append(durationpy.from_str(pattern).total_seconds())
                        elif field in ("size", "season", "episode"):
                            where.append(f"show.{field}<=?")
                            arguments.append(int(pattern))
                        elif field == "start":
                            where.append(f"show.{field}<=?")
                            arguments.append(iso8601.parse_date(pattern))
                        elif field in ("dow"):
                            where.append("CAST(strftime('%w', show.start) AS INTEGER)<=?")
                            arguments.append(int(pattern))
                        elif field in ("hour"):
                            where.append("CAST(strftime('%H', datetime(show.start, 'localtime')) AS INTEGER)<=?")
                            arguments.append(int(pattern))
                        elif field in ("minute"):
                            where.append("CAST(strftime('%M', show.start) AS INTEGER)<=?")
                            arguments.append(int(pattern))
                        else:
                            raise ConfigurationError(f"Invalid operator {operator!r} for {field!r}.")

                    elif operator == "+":
                        if field in ("duration", "age"):
                            where.append(f"show.{field}>=?")
                            arguments.append(durationpy.from_str(pattern).total_seconds())
                        elif field in ("size", "season", "episode"):
                            where.append(f"show.{field}>=?")
                            arguments.append(int(pattern))
                        elif field == "start":
                            where.append(f"show.{field}>=?")
                            arguments.append(iso8601.parse_date(pattern))
                        elif field in ("dow"):
                            where.append("CAST(strftime('%w', show.start) AS INTEGER)>=?")
                            arguments.append(int(pattern))
                        elif field in ("hour"):
                            where.append("CAST(strftime('%H', datetime(show.start, 'localtime')) AS INTEGER)>=?")
                            arguments.append(int(pattern))
                        elif field in ("minute"):
                            where.append("CAST(strftime('%M', show.start) AS INTEGER)>=?")
                            arguments.append(int(pattern))
                        else:
                            raise ConfigurationError(f"Invalid operator {operator!r} for {field!r}.")

                    else:
                        raise ConfigurationError(f"Invalid operator: {operator!r}")

                else:
                    raise ConfigurationError("Property and filter rule expected to be separated by an operator.")

        if not include_future:
            where.append("datetime(show.start) < datetime('now')")

        query = """
            SELECT show.*, downloaded.downloaded
            FROM main.show AS show
            LEFT JOIN history.downloaded ON main.show.hash = history.downloaded.hash
        """
        if where:
            query += f"WHERE {' AND '.join(where)} "
        query += "ORDER BY show.start "
        if limit:
            query += f"LIMIT {limit} "

        cursor = self.connection.cursor()
        cursor.execute(query, arguments)
        for row in cursor:
            yield dict(row)  # type: ignore

    def downloaded(self) -> Iterator["Database.Item"]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT *
            FROM history.downloaded
            ORDER BY downloaded
            """
        )
        for row in cursor:
            yield dict(row)  # type: ignore


def show_table(shows: Iterable[Database.Item]) -> None:
    def _escape_cell(title: str, obj: Any) -> str:
        if title == "hash":
            return str(obj)[:11]
        if title in ["episode", "season"] and obj is None:
            # return empty string rather than "None" in these columns
            return ""
        elif isinstance(obj, datetime):
            obj = obj.replace(tzinfo=utc_zone)
            with suppress(OSError, OverflowError):
                obj = obj.astimezone(None)

            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return str(re.sub(r"(\d+)", r" \1", durationpy.to_str(obj, extended=True)).strip())
        else:
            return str(obj)

    headers = [
        "hash",
        "channel",
        "title",
        "topic",
        "size",
        "start",
        "duration",
        "age",
        "region",
        "downloaded",
        "season",
        "episode",
    ]

    # noinspection PyTypeChecker
    table = Table(box=box.MINIMAL_DOUBLE_HEAD)
    for h in headers:
        table.add_column(h)
    for row in shows:
        table.add_row(*[_escape_cell(t, row.get(t)) for t in headers])
    console.print(table)


class Downloader:
    """Download a show from the internet."""

    Quality = Literal["url_http", "url_http_hd", "url_http_small"]

    def __init__(self, show: Database.Item):
        self.show = show

    @property
    def label(self) -> str:
        return "{title!r} ({channel}, {topic!r}, {start}, {hash:.11})".format(**self.show)

    def _download_files(self, destination_dir_path: Path, target_urls: list[str]) -> Iterable[Path]:
        file_sizes = []
        with progress_bar() as progress:
            bar_id = progress.add_task(description=f"Downloading {self.label}")

            for url in target_urls:
                response: http.client.HTTPResponse = urllib.request.urlopen(url, timeout=60, context=SSL_CONTEXT)

                # determine file size for progressbar
                file_sizes.append(int(response.getheader("content-length") or 0))
                progress.update(bar_id, total=sum(file_sizes) / len(file_sizes) * len(target_urls))

                # determine file name and destination
                destination_file_path = destination_dir_path / escape_path(Path(url).name or "unknown")

                # actual download
                with destination_file_path.open("wb") as fh:
                    while True:
                        data = response.read(CHUNK_SIZE)
                        if not data:
                            break
                        else:
                            progress.update(bar_id, advance=len(data))
                            fh.write(data)

                yield destination_file_path

    def _create_strm_files(self, destination_dir_path: Path, target_urls: list[str]) -> Iterable[Path]:
        for url in target_urls:
            file_name = Path(url).with_suffix(".strm").name
            destination_file_path = destination_dir_path / file_name

            with destination_file_path.open("w") as fh:
                fh.write(url)

        yield destination_file_path

    def _move_to_user_target(
        self,
        source_path: Path,
        target: Path,
        file_name: str,
        file_extension: str,
        media_type: str,
    ) -> Literal[False] | Path:
        posix_target = target.as_posix()
        if "{ext}" not in posix_target:
            posix_target += "{ext}"

        escaped_show_details = {k: escape_path(str(v)) for k, v in self.show.items()}
        escaped_show_details["season"] = "00" if self.show["season"] is None else f"{self.show['season']:02d}"
        escaped_show_details["episode"] = "00" if self.show["episode"] is None else f"{self.show['episode']:02d}"
        destination_file_path = Path(
            posix_target.format(
                dir=Path.cwd().as_posix(),
                filename=file_name,
                ext=file_extension,
                date=self.show["start"].date().isoformat(),
                time=self.show["start"].strftime("%H-%M"),
                **escaped_show_details,
            )
        )

        destination_file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(source_path.as_posix(), destination_file_path)
        except OSError as e:
            logger.warning("Skipped %s. Moving %r to %r failed: %s", self.label, source_path, destination_file_path, e)
        else:
            logger.info("Saved %s %s to %r.", media_type, self.label, destination_file_path)
            return destination_file_path

        return False

    @staticmethod
    def _get_m3u8_segments(base_url: str, m3u8_file_path: Path) -> Iterator[dict[str, Any]]:
        with m3u8_file_path.open("r+", encoding="utf-8") as fh:
            segment: dict[str, Any] = {}
            for line in fh:
                if not line:
                    continue
                elif line.startswith("#EXT-X-STREAM-INF:"):
                    # see http://archive.is/Pe9Pt#section-4.3.4.2
                    segment = {m.group(1).lower(): m.group(2).strip() for m in re.finditer(r"([A-Z-]+)=([^,]+)", line)}
                    for key, value in segment.items():
                        if value[0] in ('"', "'") and value[0] == value[-1]:
                            segment[key] = value[1:-1]
                        else:
                            with suppress(ValueError):
                                segment[key] = int(value)

                elif not line.startswith("#"):
                    segment["url"] = urllib.parse.urljoin(base_url, line.strip())
                    yield segment
                    segment = {}

    def _download_hls_target(
        self,
        m3u8_segments: list[dict[str, Any]],
        temp_dir_path: Path,
        base_url: str,
        quality_preference: tuple[str, str, str],
    ) -> Path:
        hls_index_segments = sorted(
            [s for s in m3u8_segments if "mp4a" not in s.get("codecs", {}) and s.get("bandwidth")],
            key=lambda s: s.get("bandwidth", 0),
        )

        # select the wanted stream
        if quality_preference[0] == "_hd":
            designated_index_segment = hls_index_segments[-1]
        elif quality_preference[0] == "_small":
            designated_index_segment = hls_index_segments[0]
        else:
            designated_index_segment = hls_index_segments[len(hls_index_segments) // 2]

        designated_index_file = next(iter(self._download_files(temp_dir_path, [designated_index_segment["url"]])))
        logger.debug(
            "Selected HLS bandwidth is %d (available: %s).",
            designated_index_segment["bandwidth"],
            ", ".join(str(s["bandwidth"]) for s in hls_index_segments),
        )

        # get stream segments
        hls_target_segments = list(self._get_m3u8_segments(base_url, designated_index_file))
        hls_target_files = self._download_files(temp_dir_path, list(s["url"] for s in hls_target_segments))
        logger.debug("%d HLS segments to download.", len(hls_target_segments))

        # download and join the segment files
        with NamedTemporaryFile(mode="wb", prefix=".tmp", dir=temp_dir_path, delete=False) as out_fh:
            temp_file_path = Path(temp_dir_path) / out_fh.name
            for segment_file_path in hls_target_files:
                with segment_file_path.open("rb") as in_fh:
                    out_fh.write(in_fh.read())

                # delete the segment file immediately to save disk space
                segment_file_path.unlink()

        return temp_file_path

    def _download_m3u8_target(self, m3u8_segments: list[dict[str, Any]], temp_dir_path: Path) -> Path:
        # get segments
        hls_target_files = self._download_files(temp_dir_path, list(s["url"] for s in m3u8_segments))
        logger.debug("%d m3u8 segments to download.", len(m3u8_segments))

        # download and join the segment files
        with NamedTemporaryFile(mode="wb", prefix=".tmp", dir=temp_dir_path, delete=False) as out_fh:
            temp_file_path = Path(temp_dir_path) / out_fh.name

            for segment_file_path in hls_target_files:
                with segment_file_path.open("rb") as in_fh:
                    out_fh.write(in_fh.read())

                # delete the segment file immediately to save disk space
                segment_file_path.unlink()

        return temp_file_path

    @staticmethod
    def _convert_subtitles_xml_to_srt(subtitles_xml_path: Path) -> Path:
        subtitles_srt_path = subtitles_xml_path.parent / (subtitles_xml_path.stem + ".srt")
        soup = BeautifulSoup(subtitles_xml_path.read_text(encoding="utf-8"), "html.parser")

        colour_to_rgb = {
            "textBlack": "#000000",
            "textRed": "#FF0000",
            "textGreen": "#00FF00",
            "textYellow": "#FFFF00",
            "textBlue": "#0000FF",
            "textMagenta": "#FF00FF",
            "textCyan": "#00FFFF",
            "textWhite": "#FFFFFF",
            "S1": "#000000",
            "S2": "#FF0000",
            "S3": "#00FF00",
            "S4": "#FFFF00",
            "S5": "#0000FF",
            "S6": "#FF00FF",
            "S7": "#00FFFF",
            "S8": "#FFFFFF",
        }

        def font_colour(text: str, colour: str) -> str:
            return f'<font color="{colour_to_rgb[colour]}">{text}</font>\n'

        def convert_time(t: str) -> str:
            t = t.replace(".", ",")
            t = re.sub(r"^1", "0", t)
            return t

        with subtitles_srt_path.open("w", encoding="utf-8") as srt:
            for p_tag in soup.findAll("tt:p"):
                # noinspection PyBroadException
                try:
                    srt.write(str(int(re.sub(r"\D", "", p_tag.get("xml:id"))) + 1) + "\n")
                    srt.write(f"{convert_time(p_tag['begin'])} --> {convert_time(p_tag['end'])}\n")
                    for span_tag in p_tag.findAll("tt:span"):
                        srt.write(font_colour(span_tag.text, span_tag.get("style")).replace("&apos", "'"))
                    srt.write("\n")
                except Exception as e:
                    logger.debug("Unexpected data in subtitle xml tag %r: %s", p_tag, e)

        return subtitles_srt_path

    def download(
        self,
        quality: tuple[Quality, Quality, Quality],
        target: Path,
        *,
        include_subtitles: bool = True,
        include_nfo: bool = True,
        set_file_modification_date: bool = False,
        create_strm_files: bool = False,
        series_mode: bool = False,
    ) -> Path | None:
        temp_path = Path(tempfile.mkdtemp(prefix=".tmp"))
        try:
            # show url based on quality preference
            show_url = self.show[quality[0]] or self.show[quality[1]] or self.show[quality[2]]

            if not show_url:
                logger.error("No valid url to download %r", self.label)
                return None

            logger.debug("Downloading %s from %r.", self.label, show_url)

            if not create_strm_files:
                show_file_path = next(iter(self._download_files(temp_path, [show_url])))
            else:
                show_file_path = next(iter(self._create_strm_files(temp_path, [show_url])))

            if set_file_modification_date and self.show["start"]:
                os.utime(
                    show_file_path,
                    (
                        self.show["start"].replace(tzinfo=timezone.utc).timestamp(),
                        self.show["start"].replace(tzinfo=timezone.utc).timestamp(),
                    ),
                )

            show_file_name = show_file_path.name
            if "." in show_file_name:
                show_file_extension = show_file_path.suffix
                show_file_name = show_file_path.stem
            else:
                show_file_extension = ""

            if show_file_extension in (".mp4", ".flv", ".mp3", ".strm"):
                final_show_file = self._move_to_user_target(
                    show_file_path, target, show_file_name, show_file_extension, "show"
                )
                if not final_show_file:
                    return None

            elif show_file_extension == ".m3u8":
                m3u8_segments = list(self._get_m3u8_segments(show_url, show_file_path))
                if any("codecs" in s for s in m3u8_segments):
                    ts_file_path = self._download_hls_target(m3u8_segments, temp_path, show_url, quality)
                else:
                    ts_file_path = self._download_m3u8_target(m3u8_segments, temp_path)
                final_show_file = self._move_to_user_target(ts_file_path, target, show_file_name, ".ts", "show")
                if not final_show_file:
                    return None

            else:
                logger.error("File extension %s of %s not supported.", show_file_extension, self.label)
                return None

            if include_subtitles and self.show["url_subtitles"]:
                logger.debug("Downloading subtitles for %s from %r.", self.label, self.show["url_subtitles"])
                try:
                    subtitles_xml_path = next(iter(self._download_files(temp_path, [self.show["url_subtitles"]])))
                except urllib.error.HTTPError:
                    logger.warning("Missing subtitles for %s.", self.label)
                else:
                    subtitles_srt_path = self._convert_subtitles_xml_to_srt(subtitles_xml_path)
                    self._move_to_user_target(subtitles_srt_path, target, show_file_name, ".srt", "subtitles")

            if include_nfo:
                root_node = "movie" if not series_mode else "episodedetails"
                nfo_movie = Et.fromstring(f'<?xml version="1.0" encoding="UTF-8" standalone="yes" ?><{root_node}/>')
                nfo_id = Et.SubElement(nfo_movie, "uniqueid")
                nfo_id.set("type", "hash")
                nfo_id.text = self.show["hash"]
                Et.SubElement(nfo_movie, "title").text = self.show["title"]
                Et.SubElement(nfo_movie, "tagline").text = self.show["topic"]
                Et.SubElement(nfo_movie, "plot").text = self.show["description"]
                Et.SubElement(nfo_movie, "studio").text = self.show["channel"]
                if self.show["start"]:
                    Et.SubElement(nfo_movie, "aired").text = self.show["start"].isoformat()
                Et.SubElement(nfo_movie, "country").text = self.show["region"]
                if series_mode and self.show["season"] is not None and self.show["episode"] is not None:
                    Et.SubElement(nfo_movie, "season").text = str(self.show["season"])
                    Et.SubElement(nfo_movie, "episode").text = str(self.show["episode"])

                with NamedTemporaryFile(mode="wb", prefix=".tmp", dir=temp_path, delete=False) as out_fh:
                    nfo_path = Path(temp_path) / out_fh.name
                    out_fh.write(Et.tostring(nfo_movie, xml_declaration=True, encoding="UTF-8"))

                nfo_path.chmod(0o644)
                self._move_to_user_target(nfo_path, target, show_file_name, ".nfo", "nfo")

            return final_show_file

        except (urllib.error.HTTPError, OSError, ValueError) as e:
            logger.error("Download of %s failed: %s", self.label, e)
        finally:
            shutil.rmtree(temp_path)

        return None


def _guess_series_details(title: str, manual_season: int = 1) -> tuple[int | None, int | None]:
    """Heuristics to extract season and episode information from the title.

    Examples with season and episode information:
        >>> _guess_series_details("Folge 4: Mehr als eine Entscheidung (S01/E04) - Audiodeskription")
        (1, 4)
        >>> _guess_series_details("Können wir das Pferd retten? (Staffel 33 Folge 10) ")
        (33, 10)
        >>> _guess_series_details("Hörfassung: Folge 3: Besuch aus dem Jenseits - Staffel 5")
        (5, 3)
        >>> _guess_series_details("Nr. 47  | Episode 3 von 4 | Staffel 1")
        (1, 3)

    Examples without season information:
        >>> _guess_series_details("#45 Trostloser VfB-Auftritt in Dortmund")
        (1, 45)

        expect the tool to find the first pattern if multiple are present
        >>> _guess_series_details("StarStarSpace #23/Japanoschlampen #34 - Die verschollene Episode")
        (1, 23)
        >>> _guess_series_details("Folge 7 (OmU) - Originalfassung mit deutschen Untertiteln")
        (1, 7)

        prefer "12. Folge" over "(1)"
        >>> _guess_series_details("Rückblick: 12. Folge - Zwillingszauber (1)")
        (1, 12)
        >>> _guess_series_details("Folge 9 ")
        (1, 9)

        don't trigger on the "#delikat" prefix
        >>> _guess_series_details("#delikatdelikat Folge 06 - Late Night Alter")
        (1, 6)

        ignore the 36, as this is the total number of episodes
        >>> _guess_series_details("Folge 11/36")
        (1, 11)
        >>> _guess_series_details('Gipfeltour (4/5) | Serie "Nix wie raus …. Madeira"')
        (1, 4)
        >>> _guess_series_details("Leichter leben (10) | Partnervermittlung für ältere Menschen")
        (1, 10)
        >>> _guess_series_details("Episode 5 - Eine Serie von Natalie Scharf")
        (1, 5)
        >>> _guess_series_details("Episode 3245")
        (1, 3245)
        >>> _guess_series_details("Helvetica (Episode 2 von 6)")
        (1, 2)
        >>> _guess_series_details("1077. Der Code-Knacker")
        (1, 1077)

    Examples of titles that cannot be parsed:
        >>> _guess_series_details("Krieg in Europa: Tag 14")
        (None, None)
        >>> _guess_series_details("Der 13. Tag")
        (None, None)
        >>> _guess_series_details("Songwriterin des Jahres 2017")
        (None, None)
    """
    # Patterns that contain a season number.
    # Using "S" to identify the season and "E" to identify the episode in the regex
    # as the order of these changes.
    season_patterns = [
        # "(S01/E01)"
        re.compile(r"\(S(?P<S>\d+)/E(?P<E>\d+)\)"),
        # "Staffel 01" "Folge 01"
        re.compile(r"Staffel (?P<S>\d+).*Folge (?P<E>\d+)"),
        # "Folge 01" "Staffel 01", different order
        re.compile(r"Folge (?P<E>\d+).*Staffel (?P<S>\d+)"),
    ]

    # Patterns that recognize episodes only.
    episode_patterns = [
        # "Folge 3"
        re.compile(r"Folge (\d+)"),
        # "Episode 3"
        re.compile(r"Episode (\d+)"),
        # "7. Folge"
        re.compile(r"(\d+)\. Folge"),
        # "7. Episode"
        re.compile(r"(\d+)\. Episode"),
        # "7. Name der Folge" (from start of string)
        re.compile(r"^(\d+)\.\s"),
        # "(12)"
        re.compile(r"\((\d+)\)"),
        # "(3/5)", where 3 is the episode and 5 is the total number of episodes
        re.compile(r"\((\d+)/\d+\)"),
        # "#45"
        re.compile(r"#(\d+)"),
    ]

    for pattern in season_patterns:
        m = pattern.search(title)
        if m is not None:
            return int(m["S"]), int(m["E"])

    for pattern in episode_patterns:
        m = pattern.search(title)
        if m is not None:
            return manual_season, int(m.group(1))

    return None, None


def run_post_download_hook(executable: Path, item: Database.Item, downloaded_file: Path) -> None:
    try:
        subprocess.run(
            [executable.as_posix()],
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={
                "MTV_DL_FILE": downloaded_file.as_posix(),
                "MTV_DL_HASH": item["hash"],
                "MTV_DL_CHANNEL": item["channel"],
                "MTV_DL_DESCRIPTION": item["description"],
                "MTV_DL_REGION": item["region"],
                "MTV_DL_SIZE": str(item["size"]),
                "MTV_DL_TITLE": item["title"],
                "MTV_DL_TOPIC": item["topic"],
                "MTV_DL_WEBSITE": item["website"],
                "MTV_DL_START": item["start"].isoformat(),
                "MTV_DL_DURATION": str(item["duration"].total_seconds()),
                "MTV_DL_SEASON": str(item["season"]),
                "MTV_DL_EPISODE": str(item["episode"]),
            },
            encoding="utf-8",
        )
    except subprocess.CalledProcessError as e:
        logger.error("Post-download hook %r returned with code %s:\n%s", executable, e.returncode, e.stdout)
    else:
        logger.info("Post-download hook %r returned successful.", executable)


app = typer.Typer(
    help=f"MediathekView-Commandline-Downloader v{version('mtv_dl')}",
    rich_markup_mode="markdown",
)

# Common options shared between commands
config_help = """
    Yaml formatted text file to overwrite arguments by their name. 
    
    If not defined differently, it is searched for to be in the root of the home dir (~/.mtv_dl.yml).  
    
    Example config:
    
        verbose: true
        high: true
        dir: ~/download
""".replace("\n", "\n\n")  # noqa: W291, W293
filter_argument = typer.Argument(
    help="""
        Use filter to select only the shows wanted. Syntax is always <field><operator><pattern>.
        
        The following operators and fields are available:
        
        - '='  Pattern is a search within the field value. 
        
            It's a case-insensitive regular expression for the fields 'description', 'start', 
            'dow' (day of the week), 'hour', 'minute', 'region', 'size', 'channel', 'topic', 
            'title', 'hash' and 'url'. 
            
            For the fields 'duration', 'age', 'episode', and 'season' it's a basic equality comparison.
        
        - '!=' Inverse of the '=' operator.
        
        - '+'  Pattern must be greater than the field value. 
        
            Available for the fields 'duration',
            'age', 'start', 'dow' (day of the week), 'hour', 'minute', 'size', 'episode', and 'season'.
        
        - '-'  Pattern must be less than the field value. 
        
            Available for the same fields as for the '+' operator.
        
        Pattern should be given in the same format as shown in the list command. Times (for
        'start'), time deltas (for 'duration', 'age') and numbers ('size') are parsed and
        smart compared. Day of the week ('dow') is 0-6 with Sunday=0.
        
        Examples:
        - topic='extra 3'                   (topic contains 'extra 3')
        - title!=spezial                    (title not contains 'spezial')
        - channel=ARD                       (channel contains ARD)
        - age-1mm                           (age is younger than 1 month)
        - duration+20m                      (duration longer then 20 min)
        - start+2017-07-01                  (show started after 2017-07-01)
        - start-2017-07-05T23:00:00+02:00   (show started before 2017-07-05, 23:00 CEST)
        - topic=Tatort dow=0 hour=20        (sunday night Tatort)
        
        As many filters as needed may be given as separated arguments (separated  with space).
        For a show to get considered, _all_ given filter criteria must meet.
    """.replace("\n", "\n\n")  # noqa: W291, W293
)
filter_set_option = typer.Option(
    "--sets",
    "-s",
    help="""
        A file to load different sets of filters.
            
        In commandline with a single run one can only give one set of filters. In most cases
        this means one can only select a single show to list or download with one run.  
        
        For --sets, a file should be given, where every line contains the same filter arguments
        that one would give on the commandline. The lines are filtered one after another and
        then processed together. Lines starting with '#' are treated as comment.  
        
        Every different filter set is expected to be on a new line.  
        
        A text file could look for example like this:  
          
            channel=ARD topic='extra 3' title!=spezial duration+20m  
            channel=ZDF topic='Die Anstalt' duration+45m  
            channel=ZDF topic=heute-show duration+20m  
            
        If additional filters where given through the commandline, all filter sets are extended
        by these filters. Be aware that this is not faster than running all queries separately
        but just more comfortable.
    """.replace("\n", "\n\n"),  # noqa: W291, W293,
)
include_future_option = typer.Option(
    "--include-future",
    help="Include shows that have not yet started.",
)


@app.command(name="list")
@use_yaml_config(default_value=CONFIG_FILE, param_help=config_help)
def list_command(
    list_filter: Annotated[list[str] | None, filter_argument] = None,
    filter_sets: Annotated[Path | None, filter_set_option] = None,
    count: Annotated[
        int,
        typer.Option(
            "--count",
            "-c",
            help="Limit the number of results.",
        ),
    ] = 50,
    include_future: Annotated[bool, include_future_option] = False,
) -> None:
    """Show the list of query results as ASCII table."""

    try:
        SHOWLIST.update_if_old()
        shows = chain(
            *(
                SHOWLIST.filtered(
                    rules=filter_set,
                    include_future=include_future,
                    limit=count,
                )
                for filter_set in SHOWLIST.read_filter_sets(
                    sets_file_path=filter_sets, default_filter=list_filter or []
                )
            )
        )
        show_table(shows)
    except ConfigurationError as e:
        logger.error(str(e))


@app.command(name="dump")
@use_yaml_config(default_value=CONFIG_FILE, param_help=config_help)
def dump_command(
    list_filter: Annotated[list[str] | None, filter_argument] = None,
    filter_sets: Annotated[Path | None, filter_set_option] = None,
    include_future: Annotated[bool, include_future_option] = False,
) -> None:
    """Show the list of query results as JSON list."""

    try:
        SHOWLIST.update_if_old()
        shows = chain(
            *(
                SHOWLIST.filtered(
                    rules=filter_set,
                    include_future=include_future,
                    limit=None,
                )
                for filter_set in SHOWLIST.read_filter_sets(
                    sets_file_path=filter_sets, default_filter=list_filter or []
                )
            )
        )
        print(json.dumps(list(shows), default=serialize_for_json, indent=4, sort_keys=True))
    except ConfigurationError as e:
        logger.error(str(e))


@app.command(name="download")
@use_yaml_config(default_value=CONFIG_FILE, param_help=config_help)
def download_command(
    list_filter: Annotated[list[str] | None, filter_argument] = None,
    filter_sets: Annotated[Path | None, filter_set_option] = None,
    high: Annotated[
        bool,
        typer.Option(
            "--high",
            "-h",
            help="Download the best available version.",
        ),
    ] = False,
    low: Annotated[
        bool,
        typer.Option(
            "--low",
            "-l",
            help="Download the smallest available version.",
        ),
    ] = False,
    oblivious: Annotated[
        bool,
        typer.Option(
            "--oblivious",
            "-o",
            help="Download even if the show already is marked as downloaded.",
        ),
    ] = False,
    target: Annotated[
        Path,
        typer.Option(
            "--target",
            "-t",
            help="""
                Directory to put the downloaded files in.
                
                May contain the parameters {dir} (from the option --dir),
                {filename} (from server filename) and {ext} (file
                name extension including the dot), and all fields from
                the listing plus {date} and {time} (the single parts
                of {start}). 
                
                If {ext} is not in the definition, it's appended automatically.
            """,  # noqa: W291, W293
        ),
    ] = Path("{dir}/{channel}/{topic}/{start} {title}{ext}"),
    mark_only: Annotated[
        bool,
        typer.Option(
            "--mark-only",
            help="""
                Do not download any show, but mark it as downloaded in the history.
                
                This is to initialize a new filter if upcoming shows are wanted.
            """,  # noqa: W293
        ),
    ] = False,
    strm: Annotated[
        bool,
        typer.Option(
            "--strm",
            help="Create .strm files instead of downloading media.",
        ),
    ] = False,
    no_subtitles: Annotated[
        bool,
        typer.Option("--no-subtitles", help="Do not try to download subtitles."),
    ] = False,
    no_nfo: Annotated[
        bool,
        typer.Option("--no-nfo", help="Do not create nfo files."),
    ] = False,
    set_file_mod_time: Annotated[
        bool,
        typer.Option(
            "--set-file-mod-time",
            help="Set the file modification time of the downloaded show to the aired date (if available).",
        ),
    ] = False,
    series: Annotated[
        bool,
        typer.Option(
            "--series",
            help="Mark the show as series in the nfo file, add season and episode information.",
        ),
    ] = False,
    post_download: Annotated[
        str | None,
        typer.Option(
            "--post-download",
            help="""
                Program to run after a download has finished.
                
                Details about the downloaded how are given via
                environment variables: FILE, HASH, CHANNEL, DESCRIPTION, 
                REGION, SIZE, TITLE, TOPIC, WEBSITE, START, and DURATION 
                (all prefixed with MTV_DL_).
            """,  # noqa: W291, W293
        ),
    ] = None,
    include_future: Annotated[bool, include_future_option] = False,
) -> None:
    """Download shows in the list of query results."""

    if not list_filter and not filter_sets:
        logger.error("At least one filter set is required.")
        sys.exit(1)
    try:
        SHOWLIST.update_if_old()
        shows = chain(
            *(
                SHOWLIST.filtered(
                    rules=filter_set,
                    include_future=include_future,
                    limit=None,
                )
                for filter_set in SHOWLIST.read_filter_sets(
                    sets_file_path=filter_sets, default_filter=list_filter or []
                )
            )
        )

        for item in shows:
            downloader = Downloader(item)
            if not downloader.show.get("downloaded") or oblivious:
                if not mark_only:
                    if high:
                        quality_preference = ("url_http_hd", "url_http", "url_http_small")
                    elif low:
                        quality_preference = ("url_http_small", "url_http", "url_http_hd")
                    else:
                        quality_preference = ("url_http", "url_http_hd", "url_http_small")
                    downloaded_file = downloader.download(
                        quality=quality_preference,  # type: ignore
                        target=target.expanduser(),
                        include_subtitles=not no_subtitles,
                        include_nfo=not no_nfo,
                        set_file_modification_date=set_file_mod_time,
                        create_strm_files=strm,
                        series_mode=series,
                    )
                    if downloaded_file:
                        SHOWLIST.add_to_downloaded(item)
                        if post_download:
                            executable = Path(post_download).expanduser()
                            run_post_download_hook(executable, item, downloaded_file)
                else:
                    SHOWLIST.add_to_downloaded(downloader.show)
                    logger.info("Marked %s as downloaded.", downloader.label)
            else:
                logger.debug("Skipping %s (already loaded on %s)", downloader.label, item["downloaded"])

    except ConfigurationError as e:
        logger.error(str(e))


@app.command(name="history")
@use_yaml_config(default_value=CONFIG_FILE, param_help=config_help)
def history_command(
    reset: Annotated[
        bool,
        typer.Option("--reset", help="Reset the list of downloaded shows."),
    ] = False,
    remove: Annotated[
        str | None,
        typer.Option("--remove", help="Remove a single show from the history."),
    ] = None,
) -> None:
    """Show the list of downloaded shows."""

    SHOWLIST.update_if_old()
    if reset:
        SHOWLIST.purge_downloaded()
    elif remove:
        SHOWLIST.remove_from_downloaded(show_hash=remove)
    else:
        show_table(SHOWLIST.downloaded())


@app.callback()
@use_yaml_config(default_value=CONFIG_FILE, param_help=config_help)
def setup(
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Show more details.",
        ),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Hide everything not really needed.",
        ),
    ] = False,
    no_bar: Annotated[
        bool,
        typer.Option(
            "--no-bar",
            "-b",
            help="Hide the progress bar.",
        ),
    ] = False,
    logfile: Annotated[
        str | None,
        typer.Option(
            "--logfile",
            "-l",
            help="Log messages to a file instead of stdout.",
        ),
    ] = None,
    use_certifi: Annotated[
        bool,
        typer.Option(
            "--certifi",
            help="Use certifi instead of builtin SSL certificates.",
        ),
    ] = False,
    dir: Annotated[  # noqa: A002
        Path | None,
        typer.Option(
            "--dir",
            "-d",
            help="Directory to put the databases in (default is the current working directory).",
        ),
    ] = None,
    refresh_after: Annotated[
        int,
        typer.Option(
            "--refresh-after",
            "-r",
            help="Update database if it is older than the given number of hours.",
        ),
    ] = 3,
) -> None:
    """Prepare the environment for the commands."""

    # mute third party modules
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # ISO8601 logger
    if logfile:
        logging_handler: logging.Handler = logging.FileHandler(Path(logfile).expanduser(), encoding="utf-8")
        logging_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)-8s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )
    else:
        logging_handler = RichHandler(
            console=console,
            show_path=False,
            omit_repeated_times=False,
        )
        logging_handler.setFormatter(logging.Formatter(datefmt="%Y-%m-%dT%H:%M:%S%z "))

    logger.addHandler(logging_handler)
    sys.excepthook = lambda _c, _e, _t: logger.critical("%s: %s\n%s", _c, _e, "".join(traceback.format_tb(_t)))

    # progressbar handling
    global HIDE_PROGRESSBAR
    HIDE_PROGRESSBAR = bool(logfile) or no_bar or quiet

    global SSL_CONTEXT
    if use_certifi:
        SSL_CONTEXT = create_default_context(cafile=certifi.where())

    if verbose:
        logger.setLevel(logging.DEBUG)
    elif quiet:
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.INFO)

    # temp file and download config
    cw_dir = Path(dir).expanduser().absolute() if dir else Path.cwd()
    cw_dir.mkdir(parents=True, exist_ok=True)
    chdir(cw_dir)
    tempfile.tempdir = cw_dir.as_posix()

    global SHOWLIST
    SHOWLIST = Database(
        filmliste=cw_dir / FILMLISTE_DATABASE_FILE,
        filmliste_refresh_after=timedelta(hours=refresh_after),
        history=cw_dir / HISTORY_DATABASE_FILE,
    )


if __name__ == "__main__":
    app()
