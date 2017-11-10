'''
warcprox/dedup.py - identical payload digest deduplication using sqlite db

Copyright (C) 2013-2017 Internet Archive

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301,
USA.
'''

from __future__ import absolute_import

import logging
import os
import json
from hanzo import warctools
import warcprox
import sqlite3
import requests
import doublethink
import rethinkdb as r
import datetime
import urllib3
from urllib3.exceptions import HTTPError

urllib3.disable_warnings()

class DedupDb(object):
    logger = logging.getLogger("warcprox.dedup.DedupDb")

    def __init__(
            self, file='./warcprox.sqlite', options=warcprox.Options()):
        self.file = file
        self.options = options

    def start(self):
        if os.path.exists(self.file):
            self.logger.info(
                    'opening existing deduplication database %s',
                    self.file)
        else:
            self.logger.info(
                    'creating new deduplication database %s', self.file)

        conn = sqlite3.connect(self.file)
        conn.execute(
                'create table if not exists dedup ('
                '  key varchar(300) primary key,'
                '  value varchar(4000)'
                ');')
        conn.commit()
        conn.close()

    def save(self, digest_key, response_record, bucket=""):
        record_id = response_record.get_header(warctools.WarcRecord.ID).decode('latin1')
        url = response_record.get_header(warctools.WarcRecord.URL).decode('latin1')
        date = response_record.get_header(warctools.WarcRecord.DATE).decode('latin1')

        key = digest_key.decode('utf-8') + "|" + bucket

        py_value = {'id':record_id, 'url':url, 'date':date}
        json_value = json.dumps(py_value, separators=(',',':'))

        conn = sqlite3.connect(self.file)
        conn.execute(
                'insert or replace into dedup (key, value) values (?, ?)',
                (key, json_value))
        conn.commit()
        conn.close()
        self.logger.debug('dedup db saved %s:%s', key, json_value)

    def lookup(self, digest_key, bucket="", url=None):
        result = None
        key = digest_key.decode('utf-8') + '|' + bucket
        conn = sqlite3.connect(self.file)
        cursor = conn.execute('select value from dedup where key = ?', (key,))
        result_tuple = cursor.fetchone()
        conn.close()
        if result_tuple:
            result = json.loads(result_tuple[0])
            result['id'] = result['id'].encode('latin1')
            result['url'] = result['url'].encode('latin1')
            result['date'] = result['date'].encode('latin1')
        self.logger.debug('dedup db lookup of key=%s returning %s', key, result)
        return result

    def notify(self, recorded_url, records):
        if (records and records[0].type == b'response'
                and recorded_url.response_recorder.payload_size() > 0):
            digest_key = warcprox.digest_str(
                    recorded_url.response_recorder.payload_digest,
                    self.options.base32)
            if recorded_url.warcprox_meta and "captures-bucket" in recorded_url.warcprox_meta:
                self.save(
                        digest_key, records[0],
                        bucket=recorded_url.warcprox_meta["captures-bucket"])
            else:
                self.save(digest_key, records[0])


def decorate_with_dedup_info(dedup_db, recorded_url, base32=False):
    if (recorded_url.response_recorder
            and recorded_url.response_recorder.payload_digest
            and recorded_url.response_recorder.payload_size() > 0):
        digest_key = warcprox.digest_str(recorded_url.response_recorder.payload_digest, base32)
        if recorded_url.warcprox_meta and "captures-bucket" in recorded_url.warcprox_meta:
            recorded_url.dedup_info = dedup_db.lookup(digest_key, recorded_url.warcprox_meta["captures-bucket"],
                                                      recorded_url.url)
        else:
            recorded_url.dedup_info = dedup_db.lookup(digest_key,
                                                      url=recorded_url.url)

class RethinkDedupDb:
    logger = logging.getLogger("warcprox.dedup.RethinkDedupDb")

    def __init__(self, options=warcprox.Options()):
        parsed = doublethink.parse_rethinkdb_url(options.rethinkdb_dedup_url)
        self.rr = doublethink.Rethinker(
                servers=parsed.hosts, db=parsed.database)
        self.table = parsed.table
        self._ensure_db_table()
        self.options = options

    def _ensure_db_table(self):
        dbs = self.rr.db_list().run()
        if not self.rr.dbname in dbs:
            self.logger.info("creating rethinkdb database %r", self.rr.dbname)
            self.rr.db_create(self.rr.dbname).run()
        tables = self.rr.table_list().run()
        if not self.table in tables:
            self.logger.info(
                    "creating rethinkdb table %r in database %r shards=%r "
                    "replicas=%r", self.table, self.rr.dbname,
                    len(self.rr.servers), min(3, len(self.rr.servers)))
            self.rr.table_create(
                    self.table, primary_key="key", shards=len(self.rr.servers),
                    replicas=min(3, len(self.rr.servers))).run()

    def start(self):
        pass

    def save(self, digest_key, response_record, bucket=""):
        k = digest_key.decode("utf-8") if isinstance(digest_key, bytes) else digest_key
        k = "{}|{}".format(k, bucket)
        record_id = response_record.get_header(warctools.WarcRecord.ID).decode('latin1')
        url = response_record.get_header(warctools.WarcRecord.URL).decode('latin1')
        date = response_record.get_header(warctools.WarcRecord.DATE).decode('latin1')
        record = {'key':k,'url':url,'date':date,'id':record_id}
        result = self.rr.table(self.table).insert(
                record, conflict="replace").run()
        if sorted(result.values()) != [0,0,0,0,0,1] and [result["deleted"],result["skipped"],result["errors"]] != [0,0,0]:
            raise Exception("unexpected result %s saving %s", result, record)
        self.logger.debug('dedup db saved %s:%s', k, record)

    def lookup(self, digest_key, bucket="", url=None):
        k = digest_key.decode("utf-8") if isinstance(digest_key, bytes) else digest_key
        k = "{}|{}".format(k, bucket)
        result = self.rr.table(self.table).get(k).run()
        if result:
            for x in result:
                result[x] = result[x].encode("utf-8")
        self.logger.debug('dedup db lookup of key=%s returning %s', k, result)
        return result

    def notify(self, recorded_url, records):
        if (records and records[0].type == b'response'
                and recorded_url.response_recorder.payload_size() > 0):
            digest_key = warcprox.digest_str(recorded_url.response_recorder.payload_digest,
                    self.options.base32)
            if recorded_url.warcprox_meta and "captures-bucket" in recorded_url.warcprox_meta:
                self.save(digest_key, records[0], bucket=recorded_url.warcprox_meta["captures-bucket"])
            else:
                self.save(digest_key, records[0])

class CdxServerDedup(object):
    """Query a CDX server to perform deduplication.
    """
    logger = logging.getLogger("warcprox.dedup.CdxServerDedup")
    http_pool = urllib3.PoolManager()

    def __init__(self, cdx_url="https://web.archive.org/cdx/search",
                 options=warcprox.Options()):
        self.cdx_url = cdx_url
        self.options = options

    def start(self):
        pass

    def save(self, digest_key, response_record, bucket=""):
        """Does not apply to CDX server, as it is obviously read-only.
        """
        pass

    def lookup(self, digest_key, url):
        """Compare `sha1` with SHA1 hash of fetched content (note SHA1 must be
        computed on the original content, after decoding Content-Encoding and
        Transfer-Encoding, if any), if they match, write a revisit record.

        Get only the last item (limit=-1) because Wayback Machine has special
        performance optimisation to handle that. limit < 0 is very inefficient
        in general. Maybe it could be configurable in the future.

        :param digest_key: b'sha1:<KEY-VALUE>' (prefix is optional).
            Example: b'sha1:B2LTWWPUOYAH7UIPQ7ZUPQ4VMBSVC36A'
        :param url: Target URL string
        Result must contain:
        {"url": <URL>, "date": "%Y-%m-%dT%H:%M:%SZ"}
        """
        u = url.decode("utf-8") if isinstance(url, bytes) else url
        try:
            result = self.http_pool.request('GET', self.cdx_url, fields=dict(
                url=u, fl="timestamp,digest", filter="!mimetype:warc/revisit",
                limit=-1))
            assert result.status == 200
            if isinstance(digest_key, bytes):
                dkey = digest_key
            else:
                dkey = digest_key.encode('utf-8')
            dkey = dkey[5:] if dkey.startswith(b'sha1:') else dkey
            line = result.data.strip()
            if line:
                (cdx_ts, cdx_digest) = line.split(b' ')
                if cdx_digest == dkey:
                    dt = datetime.datetime.strptime(
                            cdx_ts.decode('ascii'), '%Y%m%d%H%M%S')
                    date = dt.strftime('%Y-%m-%dT%H:%M:%SZ').encode('utf-8')
                    return dict(url=url, date=date)
        except (HTTPError, AssertionError, ValueError) as exc:
            self.logger.error('CdxServerDedup request failed for url=%s %s',
                              url, exc)
        return None

    def notify(self, recorded_url, records):
        """Since we don't save anything to CDX server, this does not apply.
        """
        pass

class TroughClient(object):
    logger = logging.getLogger("warcprox.dedup.TroughClient")

    def __init__(self, rethinkdb_trough_db_url):
        parsed = doublethink.parse_rethinkdb_url(rethinkdb_trough_db_url)
        self.rr = doublethink.Rethinker(
                servers=parsed.hosts, db=parsed.database)
        self.svcreg = doublethink.ServiceRegistry(self.rr)

    def segment_manager_url(self):
        master_node = self.svcreg.unique_service('trough-sync-master')
        assert master_node
        return master_node['url']

    def write_url(self, segment_id, schema_id='default'):
        provision_url = os.path.join(self.segment_manager_url(), 'provision')
        payload_dict = {'segment': segment_id, 'schema': schema_id}
        response = requests.post(provision_url, json=payload_dict)
        if response.status_code != 200:
            raise Exception(
                    'Received %s: %r in response to POST %s with data %s' % (
                        response.status_code, response.text, provision_url,
                        json.dumps(payload_dict)))
        result_dict = response.json()
        # assert result_dict['schema'] == schema_id  # previously provisioned?
        return result_dict['write_url']

    def read_url(self, segment_id):
        reql = self.rr.table('services').get_all(
                segment_id, index='segment').filter(
                        {'role':'trough-read'}).filter(
                                lambda svc: r.now().sub(
                                    svc['last_heartbeat']).lt(svc['ttl'])
                                ).order_by('load')
        self.logger.debug('querying rethinkdb: %r', reql)
        results = reql.run()
        if results:
            return results[0]['url']
        else:
            return None

    def schema_exists(self, schema_id):
        url = os.path.join(self.segment_manager_url(), 'schema', schema_id)
        response = requests.get(url)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            response.raise_for_status()

    def register_schema(self, schema_id, sql):
        url = '%s/schema/%s/sql' % (self.segment_manager_url(), schema_id)
        response = requests.put(url, sql)
        if response.status_code not in (201, 204):
            raise Exception(
                    'Received %s: %r in response to PUT %r with data %r' % (
                        response.status_code, response.text, sql, url))

class TroughDedupDb(object):
    '''
    https://github.com/internetarchive/trough
    '''
    logger = logging.getLogger("warcprox.dedup.TroughDedupDb")

    SCHEMA_ID = 'warcprox-dedup-v1'
    SCHEMA_SQL = ('create table dedup (\n'
                  '    digest_key varchar(100) primary key,\n'
                  '    url varchar(2100) not null,\n'
                  '    date datetime not null,\n'
                  '    id varchar(100));\n') # warc record id

    def __init__(self, options=warcprox.Options()):
        self.options = options
        self._trough_cli = TroughClient(options.rethinkdb_trough_db_url)
        self._write_url_cache = {}
        self._read_url_cache = {}

    def start(self):
        self._trough_cli.register_schema(self.SCHEMA_ID, self.SCHEMA_SQL)

    def _write_url(self, bucket):
        if not bucket in self._write_url_cache:
            self._write_url_cache[bucket] = self._trough_cli.write_url(
                    bucket, self.SCHEMA_ID)
            self.logger.info(
                    'trough dedup bucket %r write url is %r', bucket,
                    self._write_url_cache[bucket])
        return self._write_url_cache[bucket]

    def _read_url(self, bucket):
        if not self._read_url_cache.get(bucket):
            self._read_url_cache[bucket] = self._trough_cli.read_url(bucket)
            self.logger.info(
                    'trough dedup bucket %r read url is %r', bucket,
                    self._read_url_cache[bucket])
        return self._read_url_cache[bucket]

    def sql_value(self, x):
        if x is None:
            return 'null'
        elif isinstance(x, datetime.datetime):
            return 'datetime(%r)' % x.isoformat()
        elif isinstance(x, bool):
            return int(x)
        elif isinstance(x, str) or isinstance(x, bytes):
            # py3: repr(u'abc') => 'abc'
            #      repr(b'abc') => b'abc'
            # py2: repr(u'abc') => u'abc'
            #      repr(b'abc') => 'abc'
            # Repr gives us a prefix we don't want in different situations
            # depending on whether this is py2 or py3. Chop it off either way.
            r = repr(x)
            if r[:1] == "'":
                return r
            else:
                return r[1:]
        else:
            raise Exception("don't know how to make an sql value from %r" % x)

    def save(self, digest_key, response_record, bucket='__unspecified__'):
        write_url = self._write_url(bucket)
        record_id = response_record.get_header(warctools.WarcRecord.ID)
        url = response_record.get_header(warctools.WarcRecord.URL)
        warc_date = response_record.get_header(warctools.WarcRecord.DATE)

        sql = ('insert into dedup (digest_key, url, date, id) '
               'values (%s, %s, %s, %s);') % (
                       self.sql_value(digest_key), self.sql_value(url),
                       self.sql_value(warc_date), self.sql_value(record_id))
        try:
            response = requests.post(write_url, sql)
        except:
            self.logger.error(
                    'problem with trough write url %r', write_url,
                    exc_info=True)
            del self._write_url_cache[bucket]
            return
        if response.status_code != 200:
            del self._write_url_cache[bucket]
            self.logger.warn(
                    'unexpected response %r %r %r to sql=%r',
                    response.status_code, response.reason, response.text, sql)
        else:
            self.logger.debug('posted %r to %s', sql, write_url)

    def lookup(self, digest_key, bucket='__unspecified__', url=None):
        read_url = self._read_url(bucket)
        if not read_url:
            return None
        sql = 'select * from dedup where digest_key=%s;' % (
                self.sql_value(digest_key))
        try:
            response = requests.post(read_url, sql)
        except:
            self.logger.error(
                    'problem with trough read url %r', read_url, exc_info=True)
            del self._read_url_cache[bucket]
            return None
        if response.status_code != 200:
            del self._read_url_cache[bucket]
            self.logger.warn(
                    'unexpected response %r %r %r to sql=%r',
                    response.status_code, response.reason, response.text, sql)
            return None
        self.logger.trace('got %r from query %r', response.text, sql)
        results = json.loads(response.text)
        assert len(results) <= 1  # sanity check (digest_key is primary key)
        if results:
            result = results[0]
            result['id'] = result['id'].encode('ascii')
            result['url'] = result['url'].encode('ascii')
            result['date'] = result['date'].encode('ascii')
            self.logger.debug(
                    'trough lookup of key=%r returning %r', digest_key, result)
            return result
        else:
            return None

    def notify(self, recorded_url, records):
        if (records[0].get_header(warctools.WarcRecord.TYPE) == warctools.WarcRecord.RESPONSE
                and recorded_url.response_recorder.payload_size() > 0):
            digest_key = warcprox.digest_str(
                    recorded_url.response_recorder.payload_digest,
                    self.options.base32)
            if recorded_url.warcprox_meta and 'captures-bucket' in recorded_url.warcprox_meta:
                self.save(
                        digest_key, records[0],
                        bucket=recorded_url.warcprox_meta['captures-bucket'])
            else:
                self.save(digest_key, records[0])
