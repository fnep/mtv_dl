MediathekView Downloader
========================

A Commandline tool to download videos from public broadcasting services in germany. It's name is a reference to the `MediathekView project <https://github.com/mediathekview/MediathekView>`_, because they are doing the actual work to provide a database of availabe shows and download sources (this is just a small helper script). Unfortunately, their client requires Java and its not so easy to automate downloads. This tools aims to make it easier to download your favorite shows to your local or network storage using a cronjob. 

Features
--------

- No GUI or web interface. Less then 100 lines of code. Only python dependencies.
- Powerfull filter system for lists and download selection
- Download mp4, flv and hls media.
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
    
    
Download all shows matching any filter in `sets.txt`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: 

  $ cat sets.txt
  channel=ARD topic='extra 3' title!=spezial duration+20m
  channel=ZDF topic='Die Anstalt' duration+45m
  channel=ZDF topic=heute-show duration+20m    

.. code:: 

  $ mtv_dl download --dir=/media --high --target='{dir}/{channel}/[{topic} {date}] {title}{ext}' --sets=sets.txt
    
    
    
Installation
------------

Requirements:

- python3.5 or later
- eversthing in requirements.txt

The easiest way is to install unsing pip:

.. code:: shell

  $ pip3 install mtv_dl
