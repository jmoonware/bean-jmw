from beanjmw.importers.ofx import ofx_general
from beanjmw.importers.filters import assign
from beancount.ingest.cache import _FileMemo
from beancount.parser.parser import parse_string
from beancount.parser.printer import format_entry
from ofxparse import OfxParser

t_examples=['test1.qfx']
t_accounts=['Assets:US:BofA:Checking']
t_acct_nums=['6789']
t_lengths=[16]

import os
import glob
# this discovers test dir if in path tree
cpaths=[r for r,d,f in os.walk(os.path.abspath('.')) if t_examples[0] in f]
if len(cpaths)>0:
	cpath=cpaths[0]

def test_Init():
	for qex,qacct,ql,qn in zip(t_examples,t_accounts,t_lengths,t_acct_nums):
		qi = ofx_general.Importer(account_name=qacct,account_number=qn)
		assert qi
		fm = _FileMemo(os.path.join(cpath,qex))
		assert qi.identify(fm)
		entries = qi.extract(fm)
		assert len(entries)==ql
#		print('\n'.join([format_entry(e) for e in entries]))
		# this is what bean-check does mostly
		parse_entries,errs,config=parse_string('\n'.join([format_entry(e) for e in entries]))
		assert len(errs)==0
		assert len(parse_entries)==ql

def test_Read():
	for qex in t_examples:
		with open(os.path.join(cpath,qex),'r') as f:
			qf = OfxParser.parse(f)
		assert qf
