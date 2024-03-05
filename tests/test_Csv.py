from beanjmw.bci import process_extracted_entries, filter_entries
import beanjmw.bci as bci
from beanjmw.importers.csv import csv_general
from beanjmw.importers.qif import qif_importer
from beanjmw.importers.filters import assign
from beancount.ingest.cache import _FileMemo
from beancount.parser.parser import parse_string
from beancount.parser.printer import format_entry
from beancount.ops import validation
from beancount.parser import options
import shutil
import os, glob

# note lex order
t_examples=['discover6789.csv']
t_accounts=['Liabilities:US:Discover:D6789']
t_acct_nums=['6789']
# number of validation errors in non-processed entries
t_errs=[29] 
t_imp = [csv_general]
acct_filter="Discover"

# this discovers test dir if in path tree
cpaths=[r for r,d,f in os.walk(os.path.abspath('.')) if t_examples[0] in f]
if len(cpaths)>0:
	cpath=cpaths[0]

def test_ImportCsv():
	file_entries = []
	for tf,ta,tn,en,imp in zip(t_examples,t_accounts,t_acct_nums,t_errs,t_imp):
		importer = imp.Importer(ta,account_number=tn)
		assert importer
		fn = os.path.join(cpath,tf)
		fc = _FileMemo(fn)
		assert fc
		file_entries.append((fn,importer.extract(fc)))
	for (fn,entries),en in zip(file_entries,t_errs):
		valid_errors = validation.validate(entries,options.OPTIONS_DEFAULTS,None,validation.HARDCORE_VALIDATIONS)
#		print(fn)
#		[print(e.message) for e in valid_errors]
		# should be some validation errors with raw import
		assert len(valid_errors)==en
	# this re-loads all entries as found by 'identify' and as ingest would
	bci.account_filter=None
	file_entries, file_accounts = filter_entries(None)
	bci.account_filter=acct_filter
	proc_ent = process_extracted_entries(file_entries, None)
	# make re-named copies and re-process without errors
	# This will give a lot of "UNASSIGNED" accounts of course
	yaml_files = glob.glob(os.path.join(bci.importers.filters.assign.dir_path,"*_unassigned.yaml"))
	assert len(yaml_files) > 0
	# test that the generated yaml files are valid
	for yf in yaml_files:
		shutil.copy(yf, yf.replace('_unassigned',''))
	# now reprocess
	# this re-loads all entries again...
	bci.account_filter=None
	file_entries, file_accounts = filter_entries(None)
	bci.account_filter=acct_filter
	proc_assign_ent = process_extracted_entries(file_entries, None)
	# validate the combined entries from all files
	all_entries=[]
	for (fn,entries) in proc_assign_ent:
		all_entries.extend(entries)
	valid_errors = validation.validate(all_entries,options.OPTIONS_DEFAULTS,None,validation.HARDCORE_VALIDATIONS)
	[print(e.message) for e in valid_errors]
	[print(format_entry(e)) for e in all_entries]
	assert len(valid_errors)==0

