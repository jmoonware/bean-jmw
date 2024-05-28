# This just imports a Beancount ledger and runs through the filters
# Useful to combine two ledgers and deduplicate entries wrt the existing

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType, Balance
from beancount.core.number import MISSING
from beancount.loader import load_file

import os,sys, re

from datetime import datetime as dt

default_open_date='2000-01-01'

class Importer(ImporterProtocol):
	def __init__(self,account_name,import_filename='',reassign=False):
		self.import_filename=import_filename
		self.account_name=account_name
		# if true, ignore account assignment
		# generate assignment hints from current assignments
		self.reassign = reassign
		super().__init__()

	def identify(self, file):
		"""Return true if this importer matches the given file.
			Args:
				file: A cache.FileMemo instance.
			Returns:
				A boolean, true if this importer can handle this file.
		"""
		ret = False
		# check if this is a beancount ledger using load_file
		file_ext = os.path.splitext(file.name)[1]
		if '.bc' == file_ext or '.beancount' == file_ext or '.txt' == file_ext:
			entries, errs, config = load_file(file.name)
			if len(errs) == 0:
				 ret = True

		return ret
		
	def extract(self, file, existing_entries=None):
		"""Extract transactions from a file.
        Args:
          file: A cache.FileMemo instance.
          existing_entries: An optional list of existing directives 
        Returns:
          A list of new, imported directives (usually mostly Transactions)
          extracted from the file.
		"""
		entries=[]
		try:
			ledger = load_file(file.name)
			if ledger and len(ledger) > 0:
				loaded_entries = ledger[0] # That's it!
			# now, adjust transactions if we are reassigning
			for e in loaded_entries:
				if type(e) == Transaction and e.postings and self.reassign:
					accts=[p.account for p in e.postings]
					if self.account_name in accts:
						pidx = accts.index(self.account_name)
						accts.remove(self.account_name)
						# usually just 1, but could be more than one which
						# would need a hand-edit of the yaml file
						if len(accts) == 1:
							e.meta["category"]=accts[0].replace('Expenses:','')
							entries.append(e._replace(postings=[e.postings[pidx]]))
						else:
							entries.append(e)
				elif type(e) == Balance:
					# only include balace statements from this account
					if self.account_name in e.account:
						entries.append(e)
				else: # just keep any other transaction
					entries.append(e)
		except Exception as ex:
			sys.stderr.write("Unable to open or parse {0}: {1}\n".format(file.name,ex))
			return(entries)
		return(entries)

	def file_account(self, file):
		"""Return an account associated with the given file.
        Args:
          file: A cache.FileMemo instance.
        Returns:
          The name of the account that corresponds to this importer.
		"""
		return(self.account_name)

	def file_name(self, file):
		"""A filter that optionally renames a file before filing.

        This is used to make tidy filenames for filed/stored document files. If
        you don't implement this and return None, the same filename is used.
        Note that if you return a filename, a simple, RELATIVE filename must be
        returned, not an absolute filename.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          The tidied up, new filename to store it as.
		"""
		init_name=os.path.split(file.name)[1]
		return(init_name)

	def file_date(self, file):
		"""Attempt to obtain a date that corresponds to the given file.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          A date object, if successful, or None if a date could not be extracted.
          (If no date is returned, the file creation time is used. This is the
          default.)
		"""
		return

