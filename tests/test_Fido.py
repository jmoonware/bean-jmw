from beanjmw.bci import process_extracted_entries, filter_entries
import beanjmw.bci as bci
from beanjmw.importers.fido import fido_csv
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
# note same file for Fidelity (multiple accts per file)
t_examples=['fidelity.csv','fidelity.csv']
t_accounts=['Assets:US:Fidelity:FX1111','Assets:US:Fidelity:FZ2222']
t_acct_nums=['1111','2222']
# number of validation errors in non-processed entries
t_errs=[13,30] 
t_imp = [fido_csv,fido_csv]
acct_filter="Fidelity"

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
		[print(format_entry(e)) for e in entries]
		valid_errors = validation.validate(entries,options.OPTIONS_DEFAULTS,None,validation.HARDCORE_VALIDATIONS)
		[print(e.message) for e in valid_errors]
		# should be some validation errors with raw import
		assert len(valid_errors)==en
	# this re-loads all entries as found by 'identify' and as ingest would
	bci.account_filter=None
	file_entries, file_accounts = filter_entries(None)
	bci.account_filter=acct_filter
	proc_ent = process_extracted_entries(file_entries, None)
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
#	[print(e.message) for e in valid_errors]
#	[print(format_entry(e)) for e in all_entries]
	assert len(valid_errors)==0

