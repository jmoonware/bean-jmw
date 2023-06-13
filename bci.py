# 
# bci.py: custom script for beancount to ingest dowloaded files
# put all your data in a parallel directory named 'private'
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

import accts

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
	
		Use the command line argument 'existing' to assign ledger_entries
		
    Returns:
      A possibly different version of extracted_entries_list, a list of
      (filename, entries), to be printed.
    """
    # This ugly little thing is used to reconstruct accounts associated with 
    # the files in the extracted_entries_list	
	# why only the filename is returned with the entries?
    filename_accounts={}
    extracted_filenames=[x[0] for x in extracted_entries_list]
    for filename, importers in find_imports(CONFIG, extracted_filenames,logfile=None):
        file = cache.get_file(filename)
        filename_accounts[filename]=[]
        for importer in importers:
            filename_accounts[filename].append(importer.file_account(file))

    # why is there a list of importers? Not one importer per file?
    for fn in filename_accounts:
        if len(filename_accounts[fn]) > 1:
            sys.stderr.write("Found multiple accounts for {0}: {1}\n".format(fn,filename_accounts[fn]))
        # just take the first one
        filename_accounts[fn]=filename_accounts[fn][0] 

    # Now we can get the check and account assignment yaml files

	# Assign payees to check via <account>_payees.yaml file for account
    new_entries_list=[]
    for fn,entries in extracted_entries_list:
        if fn in filename_accounts.keys() and "CHECKING" in filename_accounts[fn].upper():
            new_entries_list.append((fn,assign_check_payees(entries,filename_accounts[fn],fn)))
        else:
            new_entries_list.append((fn,entries))

    # assign new accounts, and possibly open them
	# unassigned accounts will be sent to <account>_unassigned.yaml
    new_entries_list=assign_accounts(new_entries_list,ledger_entries,filename_accounts)

    # for each extracted entry, look for duplicates
    deduped_entries_list=deduplicate(new_entries_list, ledger_entries)	

    return deduped_entries_list

# Invoke the script.
scripts_utils.ingest(CONFIG, hooks=[process_extracted_entries])
