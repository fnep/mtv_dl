# MediathekView Downloader

A command line tool to download videos from public broadcasting services in Germany. It's name is a reference to the [MediathekView project](https://github.com/mediathekview/MediathekView), because they are doing the actual work to provide a database of available shows and download sources (this is just a small helper script). Unfortunately, their client requires Java and its not so easy to automate downloads. This tools aims to make it easier to download your favorite shows to your local or network storage using a cronjob.


## Features

- Commandline first interface.
- Powerful filter system for lists and download selection.
- Download .mp4, .flv and .m3u8 (HLS) media inclusive subtitles.
- Keep track of downloaded files and don't download them again.
- Template naming of the downloaded files.


## Usage examples


### Searching for shows

```shell
❯ mtv_dl list topic='extra 3' duration+20m age-1w
+----------+---------+------------------------+---------+------+---------------------------+----------+---------+--------+---------------------+
| hash     | channel | title                  | topic   | size | start                     | duration | age     | region | downloaded          |
+----------+---------+------------------------+---------+------+---------------------------+----------+---------+--------+---------------------+
| 49ea2aa7 | ARD     | Extra 3 vom 10.08.2017 | extra 3 | 646  | 2017-08-10T22:45:00+02:00 | 43m      | 14h 15m |        | None                |
+----------+---------+------------------------+---------+------+---------------------------+----------+---------+--------+---------------------+
```

### Download all shows matching the filter

```shell
❯ mtv_dl download topic='extra 3' duration+20m age-1w
```


### Download all shows matching any filter a text file

```shell
❯ cat shows.txt
channel=ARD topic='extra 3' title!=spezial duration+20m
channel=ZDF topic='Die Anstalt' duration+45m
channel=ZDF topic=heute-show duration+20m
```

```shell
❯ mtv_dl --dir=/media download --high --target='{dir}/{channel}/[{topic} {date}] {title}{ext}' --sets=shows.txt
```

### Use a config file to apply useful defaults for all commands

This is my cronjob default.

```shell
❯ cat  ~/.mtv_dl.yml
high: true
dir: /media
target: '{dir}/{channel}/[{topic} {date}] {title}{ext}'
```

```shell
❯ crontab -l
0 *	* * * mtv_dl download --logfile=~/download.log --sets=~/shows.txt
```


### Get show details in JSON format

```shell
❯ mtv_dl dump hash=49ea2aa7
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
```

## Installation


Requirements:

- python3.10 or later
- everything in pyproject.toml

The easiest way is to install using pip:

```shell
❯ python3 -m pip install mtv-dl
```

### Docker

You can use docker to run the latest version. 

```shell
❯ docker run -v ./my-data/:/data ghcr.io/fnep/mtv_dl
```

### Development


This project uses uv for building, publishing and dependencies.

To get startet checkout the repository and

```shell
❯ uv run mtv_dl
```

## Support

This project is free and open source (MIT licensed). It's not very actively maintained but also not neglected. It's just here in case it's useful for somebody. 

Für "Issues": Ich komme aus Dresden und spreche auch Deutsch.
