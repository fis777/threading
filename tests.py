import pytest
import gzip
from memcache import  Client
from os import remove
import logging
from collections import  namedtuple
import memc_load_with_threads as mt

@pytest.fixture
def setup_4_main(request):
	logging.basicConfig(filename=None, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
	mt.mc
	Options = namedtuple('Options',['dry','pattern','idfa','gaid','adid','dvid'])
	options = Options(False, '2017*.tsv.gz', '127.0.0.1:33013',"127.0.0.1:33014", "127.0.0.1:33015","127.0.0.1:33016")
	test_line = 'adid	9332883bbb2417a18f789e7b3e27e867	115.412549983	-18.0819564079	9905,6415'
	tf = gzip.open('201701.tsv.gz', 'wb')
	tf.write(test_line)
	tf.close()
	def resurce_teardown():
		remove('.201701.tsv.gz')
	request.addfinalizer(resurce_teardown)
	return options

def test_parse_appsinstalled():
	test_line = 'idfa	1rfw452y52g2gq4g	55.55	42.42	1423,43,567,3,7,23'
	ai = mt.parse_appsinstalled(test_line)
	assert ai.dev_type == 'idfa'
	assert ai.dev_id == '1rfw452y52g2gq4g'
	assert ai.lat == 55.55
	assert ai.lon == 42.42
	assert ai.apps == [1423,43,567,3,7,23]

def test_insert_appsinstalled():
	mc = Client(['127.0.0.1:33013'])
	test_line = 'idfa	1rfw452y52g2gq4g	55.55	42.42	1423,43,567,3,7,23'
	appsinstalled = mt.parse_appsinstalled(test_line)
	mt.insert_appsinstalled('127.0.0.1:33013', appsinstalled)
	assert mc.get('idfa:1rfw452y52g2gq4g') == '\x08\x8f\x0b\x08+\x08\xb7\x04\x08\x03\x08\x07\x08\x17\x11fffff\xc6K@\x19\xf6(\\\x8f\xc25E@'

def test_main(setup_4_main):
	mc = mt.mc
	mt.main(setup_4_main)
	equals_line = '\x08\xb1M\x08\x8f2\x11\xd3<\x0b8g\xda\\@\x19\xce\xa0[\x18\xfb\x142\xc0'
	result_line = mc['adid'].get('adid:9332883bbb2417a18f789e7b3e27e867')
	assert equals_line == result_line


		