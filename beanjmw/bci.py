# 
# bci.py: custom script for beancount to ingest dowloaded files
#
import sys
from os import path
import os, re
sys.path.insert(0, path.abspath(os.curdir))
sys.path.insert(0, path.join(path.dirname(__file__)))

from beancount.core import data
from beancount.core.data import Note
from beancount.ingest import scripts_utils
from beancount.ingest import extract
from beancount.ingest import cache
from beancount.ingest.identify import find_imports

from importers.filters.assign import assign_accounts, assign_check_payees
from importers.filters.assign import deduplicate, auto_open

import importers.filters.assign 
importers.filters.assign.dir_path=path.join(path.abspath(os.curdir),"yaml")
if not path.exists(importers.filters.assign.dir_path):
    os.makedirs(importers.filters.assign.dir_path)

from beancount.parser.printer import EntryPrinter

# set to True to remove duplicate entries from output
# otherwise mark them in the meta field 'mark'
remove_duplicates=True
# only import this matching account
# See argv kludge at bottom of file
account_filter=None

try:
    import accts
except ModuleNotFoundError as mnf:
    sys.stderr.write("Warning: did not find accts.py\n\tMake sure you create one in the downloads directory \n\tfrom where you should execute this module\n")
    import example_accts as accts

CONFIG = accts.CONFIG

# Override the header on extracted text (if desired).
extract.HEADER = ';; -*- mode: org; mode: beancount; coding: utf-8; -*-\n'

def filter_entries(extracted_entries_list):
    # This ugly little thing is used to reconstruct accounts associated with 
    # the files in the extracted_entries_list	
	# NOTE: This depends on the extracted_entries_list being in the same 
	# order as find_imports (lexical sort) 
    accounts = [] # same length as filtered_entries_list
    # filtered_entries_list has only the account_filter matches
    filtered_entries_list=[]

	# NOTE: This is how ingest/scripts_utils finds the file list
    # we will filter here for specific account if specified
    extracted_filenames=[path.abspath(os.curdir)]
    entryno=0
    for filename, importers in find_imports(CONFIG, extracted_filenames,logfile=None):
        file = cache.get_file(filename)
        for importer in importers:
            if extracted_entries_list:
                entries = extracted_entries_list[entryno][1]
            else:
                entries = importer.extract(file)
            entryno+=1
            acct=importer.file_account(file)
            if account_filter and not re.search(account_filter,acct):
                continue
				
            accounts.append((filename,acct))
            filtered_entries_list.append((filename,entries))
    return filtered_entries_list,accounts

def process_extracted_entries(extracted_entries_list, ledger_entries):
    """ Filter function

    Args:
      extracted_entries_list: A list of (filename, entries) pairs, where
        'entries' are the directives extract from 'filename'.
      ledger_entries: If provided, a list of directives from the existing
        ledger of the user. This is non-None if the user provided their
        ledger file as an option.
	
		Use the command line argument 'existing' ('-e') to include
		 ledger_entries
		
    Returns:
      A possibly different version of extracted_entries_list, a list of
      (filename, entries), to be printed.
    """

    filtered_entries_list, accounts = filter_entries(extracted_entries_list)
    if len(filtered_entries_list)==0:
        return([("Nothing to do",[])])
    
    # Now we can get the check and account assignment yaml files
	# Assign payees to check via <account>_payees.yaml file for account
    new_entries_list=[]
    new_accounts=[]
    for (fn,entries),(f,acct) in zip(filtered_entries_list,accounts):
        if "CHECKING" in acct.upper():
            new_entries_list.append((fn,assign_check_payees(entries,acct,fn)))
            new_accounts.append((fn,acct))
        else:
            new_entries_list.append((fn,entries))
            new_accounts.append((fn,acct))

    # assign new accounts, and possibly open them
	# unassigned accounts will be sent to <account>_unassigned.yaml
    new_entries_list=assign_accounts(new_entries_list,ledger_entries,new_accounts)

    # for each extracted entry, look for duplicates
    deduped_entries_list=deduplicate(new_entries_list, ledger_entries)	

	# remove open statements and use the auto plugin if true
	# that gets rid of "duplicate open" errors in bean-check
	# for accounts kept in separate files
    if hasattr(accts, "auto_open"):
        if accts.auto_open:
            deduped_entries_list = auto_open(deduped_entries_list)

    return deduped_entries_list

if __name__=='__main__':
	# Invoke the script.
	# Capture and remove command-line -a account_filter args
	try:
		if "-a" in sys.argv:
			idx=sys.argv.index("-a")
			account_filter=sys.argv[idx+1]
			del sys.argv[idx]
			del sys.argv[idx]
			sys.stderr.write("Using account filter {0}\n".format(account_filter))
	except Exception as ex:
		sys.stderr.write("Command line error - {0}\n".format(ex))
		sys.exit(1)

	if hasattr(accts, "auto_open"):
		print('plugin "beancount.plugins.auto"')
		# TODO: make booking method conifguable
		print('option "booking_method" "FIFO"')

	EntryPrinter.META_IGNORE.add('__residual__')
	scripts_utils.ingest(CONFIG, hooks=[process_extracted_entries])
