from qifparse.parser import QifParser

qif_example1='test1.qif'

import os
import glob
# this discovers test dir if in path tree
cpaths=[r for r,d,f in os.walk(os.path.abspath('.')) if 'test1.qif' in f]
if len(cpaths)>0:
	cpath=cpaths[0]

def test_Basic():
	with open(os.path.join(cpath,qif_example1),'r') as f:
		qf = QifParser(dayfirst=False).parse(f)
	assert qf
