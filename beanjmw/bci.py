# 
# bci.py: custom script for beancount to ingest dowloaded files
#
import sys
from os import path
import os
sys.path.insert(0, path.abspath(os.curdir))
sys.path.insert(0, path.join(path.dirname(__file__)))

from beancount.core import data
from beancount.core.data import Note
from beancount.ingest import scripts_utils
from beancount.ingest import extract
from beancount.ingest import cache
from beancount.ingest.identify import find_imports

from importers.filters.assign import assign_accounts, assign_check_payees
from importers.filters.assign import deduplicate

import importers.filters.assign 
importers.filters.assign.dir_path=path.join(path.abspath(os.curdir),"yaml")
if not path.exists(importers.filters.assign.dir_path):
    os.makedirs(importers.filters.assign.dir_path)

# set to True to remove duplicate entries from output
# otherwise mark them in the meta field 'mark'
remove_duplicates=True

try:
    import accts
except ModuleNotFoundError as mnf:
    sys.stderr.write("Warning: did not find accts.py\n\tMake sure you create one in the downloads directory \n\tfrom where you should execute this module\n")
    import example_accts as accts

CONFIG = accts.CONFIG

# Override the header on extracted text (if desired).
extract.HEADER = ';; -*- mode: org; mode: beancount; coding: utf-8; -*-\n'

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
    # This ugly little thing is used to reconstruct accounts associated with 
    # the files in the extracted_entries_list	
	# NOTE: This depends on the extracted_entries_list being in the same 
	# order as find_imports 
    accounts = [] # same length as extracted_entries_list
	# NOTE: This is how ingest/scripts_utils finds the file list
    extracted_filenames=[path.abspath(os.curdir)]
    for filename, importers in find_imports(CONFIG, extracted_filenames,logfile=None):
        file = cache.get_file(filename)
        for importer in importers:
            accounts.append((filename,importer.file_account(file)))

    # check with warning - probably should raise here 
    if len(accounts)!=len(extracted_entries_list):
        sys.stderr.write("Warning: accounts and extracted lists do not match {0} {1}\n".format(accounts,str([(x,len(y)) for x,y in extracted_entries_list])))
    # Now we can get the check and account assignment yaml files
	# Assign payees to check via <account>_payees.yaml file for account
    new_entries_list=[]
    for (fn,entries),(f,acct) in zip(extracted_entries_list,accounts):
        if f!=fn:
            sys.stderr.write("Warning: file/account mismatch {0} {1} {2}\n".format(fn,f,acct))
        if "CHECKING" in acct.upper():
            new_entries_list.append((fn,assign_check_payees(entries,acct,fn)))
        else:
            new_entries_list.append((fn,entries))

    # assign new accounts, and possibly open them
	# unassigned accounts will be sent to <account>_unassigned.yaml
    new_entries_list=assign_accounts(new_entries_list,ledger_entries,accounts)

    # for each extracted entry, look for duplicates
    deduped_entries_list=deduplicate(new_entries_list, ledger_entries)	

    return deduped_entries_list

if __name__=='main':
	# Invoke the script.
	scripts_utils.ingest(CONFIG, hooks=[process_extracted_entries])
