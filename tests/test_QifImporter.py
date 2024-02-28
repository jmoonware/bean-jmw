from qifparse.parser import QifParser

qif_examples=['test1.qif','test2.qif']

import os
import glob
# this discovers test dir if in path tree
cpaths=[r for r,d,f in os.walk(os.path.abspath('.')) if qif_examples[0] in f]
if len(cpaths)>0:
	cpath=cpaths[0]

def test_Basic():
	for qex in qif_examples:
		with open(os.path.join(cpath,qex),'r') as f:
			qf = QifParser(dayfirst=False).parse(f)
		assert qf

