# -*- coding: utf-8 -*-
"""
Deduplication handlers for warcprox

Default is local, file-based deduplication.

"""

import logging
import os
from hanzo import warctools, httptools

try:
    import dbm.gnu
    dbm_gnu = dbm.gnu
except ImportError:
    try:
        import gdbm
        dbm_gnu = gdbm
    except ImportError:
        import anydbm
        dbm_gnu = anydbm


class DedupDb(object):
    logger = logging.getLogger('warcprox.DedupDb')

    def __init__(self, dbm_file='./warcprox-dedup.db'):
        if os.path.exists(dbm_file):
            self.logger.info('opening existing deduplication database {}'.format(dbm_file))
        else:
            self.logger.info('creating new deduplication database {}'.format(dbm_file))

        self.db = dbm_gnu.open(dbm_file, 'c')

    def close(self):
        self.db.close()

    def sync(self):
        self.db.sync()

    def save(self, key, response_record, offset):
        record_id = response_record.get_header(warctools.WarcRecord.ID).decode('latin1')
        url = response_record.get_header(warctools.WarcRecord.URL).decode('latin1')
        date = response_record.get_header(warctools.WarcRecord.DATE).decode('latin1')

        py_value = {'i':record_id, 'u':url, 'd':date}
        json_value = json.dumps(py_value, separators=(',',':'))

        self.db[key] = json_value.encode('utf-8')
        self.logger.debug('dedup db saved {}:{}'.format(key, json_value))


    def lookup(self, key):
        if key in self.db:
            json_result = self.db[key]
            result = json.loads(json_result.decode('utf-8'))
            result['i'] = result['i'].encode('latin1')
            result['u'] = result['u'].encode('latin1')
            result['d'] = result['d'].encode('latin1')
            return result
        else:
            return None


class SolrDedupDb(DedupDb):
    logger = logging.getLogger('warcprox.SoldDedupDb')

    def __init__(self, solr_endpoint='http://localhost:8080/solr'):
    	self.solr_endpoint = solr_endpoint

    def close(self):
    	pass

    def sync(self):
        pass

    def save(self, key, response_record, offset):
        record_id = response_record.get_header(warctools.WarcRecord.ID).decode('latin1')
        url = response_record.get_header(warctools.WarcRecord.URL).decode('latin1')
        date = response_record.get_header(warctools.WarcRecord.DATE).decode('latin1')

#        py_value = {'i':record_id, 'u':url, 'd':date}
#        json_value = json.dumps(py_value, separators=(',',':'))#

#        self.db[key] = json_value.encode('utf-8')
#        self.logger.debug('dedup db saved {}:{}'.format(key, json_value))
#        
#        payload = {'key1': 'value1', 'key2': 'value2'}
#		r = requests.get("http://httpbin.org/get", params=payload)



    def lookup(self, key):
    	return None
#        if key in self.db:
#            json_result = self.db[key]
#            result = json.loads(json_result.decode('utf-8'))
#            result['i'] = result['i'].encode('latin1')
#            result['u'] = result['u'].encode('latin1')
#            result['d'] = result['d'].encode('latin1')
#            return result
#        else:
#            return None
