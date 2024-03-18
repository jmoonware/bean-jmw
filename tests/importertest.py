from beanjmw.bci import process_extracted_entries, filter_entries
import beanjmw.bci as bci
from beanjmw.importers.fido import fido_csv
from beanjmw.importers.qif import qif_importer
from beanjmw.importers.filters import assign
from beancount.ingest.cache import _FileMemo
from beancount.parser.parser import parse_string
from beancount.parser.printer import format_entry, EntryPrinter
from beancount.ops import validation
from beancount.parser import options
from beancount.core.data import Open
import shutil
import os, glob

def converttest(t_examples,t_accounts,t_acct_nums,t_errs,t_imp,acct_filter,output_name):
	# this discovers test dir if in path tree
	cpaths=[r for r,d,f in os.walk(os.path.abspath('.')) if t_examples[0] in f]
	if len(cpaths)>0:
		cpath=cpaths[0]

	file_entries = []
	arg_check=[len(x) for x in [t_examples,t_accounts,t_acct_nums,t_errs,t_imp]]
	assert len(t_examples)==len(t_accounts)==len(t_acct_nums)==len(t_errs)==len(t_imp),"converttest: Arg lengths don't match {0}".format(arg_check)
	for tf,ta,tn,en,imp in zip(t_examples,t_accounts,t_acct_nums,t_errs,t_imp):
		print("!!! "+tf)
		importer = imp.Importer(ta,account_number=tn)
		assert importer,"No importer"
		fn = os.path.join(cpath,tf)
		fc = _FileMemo(fn)
		assert fc,"No filememo for {0}".format(fn)
		init_len=len(file_entries)
		print("File={0}, acct={1}, an={2}, exerr={3}".format(fn,ta,tn,en))
		file_entries.append((fn,importer.extract(fc)))
		assert len(file_entries)==init_len+1,"Did not add file {0}".format(fn)
	for (fn,entries),en in zip(file_entries,t_errs):
		[print(format_entry(e)) for e in entries]
		valid_errors = validation.validate(entries,options.OPTIONS_DEFAULTS,None,validation.HARDCORE_VALIDATIONS)
		print("*** Errors")
		[print("*** " + e.message) for e in valid_errors]
		# should be some validation errors with raw import
		assert len(valid_errors)==en,"Fail: found {0} errs, expected {1}".format(len(valid_errors),en)
	# this re-loads all entries as found by 'identify' and as ingest would
	bci.account_filter=None
	file_entries, file_accounts = filter_entries(None)
	bci.account_filter=acct_filter
	proc_ent = process_extracted_entries(file_entries, None)
	# now reprocess
	# make re-named yaml copies and re-process without errors
	# This will give a lot of "UNASSIGNED" accounts of course
	# TODO: this will copy all yaml files each time - prob want only
	# account-relevant files, although this method works
	yaml_files = glob.glob(os.path.join(bci.importers.filters.assign.dir_path,"*_unassigned.yaml"))
	for yf in yaml_files:
		shutil.copy(yf, yf.replace('_unassigned',''))
	# this re-loads all entries again...
	# and tests that the (possibly generated) yaml files are valid
	bci.account_filter=None
	file_entries, file_accounts = filter_entries(None)
	bci.account_filter=acct_filter
	proc_assign_ent = process_extracted_entries(file_entries, None)
	# validate the combined entries from all files
	all_entries=[]
	for (fn,entries) in proc_assign_ent:
		all_entries.extend(entries)
	# opens must precede transactions for HARDCORE_VALIDATIONS
	# balance statments must come after opens
	all_entries.sort(key=lambda e:str(type(e)).replace("Balance", "Z"))
	valid_errors = validation.validate(all_entries,options.OPTIONS_DEFAULTS,None,validation.HARDCORE_VALIDATIONS)
	# should have no errors here
	[print("=== " + e.message) for e in valid_errors]
	# make an original if it doesn't exist
	out_path = os.path.join(cpath,output_name)
	EntryPrinter.META_IGNORE.add('__residual__')
	# don't sort by date - this keeps entries in blocks by file input
	if not os.path.isfile(out_path):
		with open(out_path,'w') as f:		
			[f.write(format_entry(e)+'\n') for e in all_entries]
	# always write out the new results for comparison
	new_out_path = os.path.join(cpath,output_name.replace('old','new'))
	with open(new_out_path,'w') as f:
		[f.write(format_entry(e)+'\n') for e in all_entries]
		
	assert len(valid_errors)==0, "All errors = {0}, should be zero".format(len(valid_errors))
	assert os.system('bean-check {0}'.format(new_out_path))==0

