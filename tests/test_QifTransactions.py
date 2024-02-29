from qifparse.parser import QifParser
from beanjmw.importers.qif import qif_importer
from beanjmw.importers.filters import assign
from beancount.ingest.cache import _FileMemo
from beancount.parser.parser import parse_string
from beancount.parser.printer import format_entry

qif_examples=['test1.qif','test2.qif','test3.qif']
qif_accounts=['Assets:US:BofA:Checking','Assets:US:Fidelity:F1234','Assets:US:BofA:Checking']
qif_lengths=[23,18,12]

import os
import glob
# this discovers test dir if in path tree
cpaths=[r for r,d,f in os.walk(os.path.abspath('.')) if qif_examples[0] in f]
if len(cpaths)>0:
	cpath=cpaths[0]

def test_Init():
	for qex,qacct,ql in zip(qif_examples,qif_accounts,qif_lengths):
		qi = qif_importer.Importer(account_name=qacct)
		assert qi
		fm = _FileMemo(os.path.join(cpath,qex))
		assert qi.identify(fm)
		entries = qi.extract(fm)
		assert len(entries)==ql
		print('\n'.join([format_entry(e) for e in entries]))
		# this is what bean-check does mostly
		parse_entries,errs,config=parse_string('\n'.join([format_entry(e) for e in entries]))
		assert len(errs)==0
		assert len(parse_entries)==ql

def test_Assign():
	for qex in qif_examples:
		with open(os.path.join(cpath,qex),'r') as f:
			qf = QifParser(dayfirst=False).parse(f)
		assert qf
