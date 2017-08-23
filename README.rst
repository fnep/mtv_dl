MediathekView Downloader
========================

A command line tool to download videos from public broadcasting services in Germany. It's name is a reference to the `MediathekView project <https://github.com/mediathekview/MediathekView>`_, because they are doing the actual work to provide a database of available shows and download sources (this is just a small helper script). Unfortunately, their client requires Java and its not so easy to automate downloads. This tools aims to make it easier to download your favorite shows to your local or network storage using a cronjob.


Features
--------

- No GUI or web interface. Less then 1000 lines of code. Only python dependencies.
- Powerful filter system for lists and download selection.
- Download .mp4, .flv and .m3u8 (HLS) media.
- Keep track of downloaded files and don't download them again.
- Template naming of the downloaded files.


Usage examples
--------------


Searching for shows
~~~~~~~~~~~~~~~~~~~

.. code::

  $ mtv_dl list topic='extra 3' duration+20m age-1w
  +----------+---------+------------------------+---------+------+---------------------------+----------+---------+--------+---------------------+
  | hash     | channel | title                  | topic   | size | start                     | duration | age     | region | downloaded          |
  +----------+---------+------------------------+---------+------+---------------------------+----------+---------+--------+---------------------+
  | 49ea2aa7 | ARD     | Extra 3 vom 10.08.2017 | extra 3 | 646  | 2017-08-10T22:45:00+02:00 | 43m      | 14h 15m |        | None                |
  +----------+---------+------------------------+---------+------+---------------------------+----------+---------+--------+---------------------+


Download all shows matching the filter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code::

  $ mtv_dl download topic='extra 3' duration+20m age-1w


Download all shows matching any filter a text file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code::

  $ cat shows.txt
  channel=ARD topic='extra 3' title!=spezial duration+20m
  channel=ZDF topic='Die Anstalt' duration+45m
  channel=ZDF topic=heute-show duration+20m

.. code::

  $ mtv_dl download --dir=/media --high --target='{dir}/{channel}/[{topic} {date}] {title}{ext}' --sets=shows.txt


Use a config file to apply useful defaults for all commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is my cronjob default.

.. code::

  $ cat  ~/.mtv_dl.yml
  high: true
  dir: /media
  target: '{dir}/{channel}/[{topic} {date}] {title}{ext}'

.. code::

  $ crontab -l
  0 *	* * * mtv_dl download --logfile=~/download.log --sets=~/shows.txt


Get show details in JSON format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code::

  $ mtv_dl dump hash=49ea2aa7
  [
      {
          "age": "17:15:00",
          "channel": "ARD",
          "description": "[...]",
          "duration": "0:43:00",
          "hash": "49ea2aa7",
          "new": false,
          "region": "",
          "size": 646,
          "start": "2017-08-10T22:45:00+02:00",
          "title": "Extra 3 vom 10.08.2017",
          "topic": "extra 3",
          "url_http": "[...]",
          "url_http_hd": "[...]",
          "url_http_small": "[...]",
          "url_subtitles": "",
          "website": "[...]"
      }
  ]

Installation
------------

Requirements:

- python3.5 or later
- everything in requirements.txt

The easiest way is to install using pip:

.. code:: shell

  $ pip3 install mtv-dl
