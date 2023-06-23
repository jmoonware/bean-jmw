# custom importer to load QIF files exported from Quicken

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET

import os,sys

# use the custom qifparse package in https://github.com/jmoonware/qifparse
from qifparse.parser import QifParser

from datetime import datetime as dt

# remove these chars as Beancount accounts can't have them
quicken_category_remove=[' ','\'','&','-','+','.']

class Importer(ImporterProtocol):
	def __init__(self,account_name,currency='USD'):
		self.account_name=account_name
		self.currency=currency
		super().__init__()

	def identify(self, file):
		"""Return true if this importer matches the given file.
			Args:
				file: A cache.FileMemo instance.
			Returns:
				A boolean, true if this importer can handle this file.
		"""
		if os.path.splitext(file.name)[1].upper()=='.QIF':
			head_lines=file.head().split('\n')
			found=False
			for ln in range(len(head_lines)):
				if '!Account' in head_lines[ln]:
					for l in head_lines[ln+1:]:
						if self.account_name in l:
							found=True
							break
			return found
		else:
			 return False
		
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
		qif=None
		with open(file.name,'r') as f:
			qif=QifParser(dayfirst=False).parse(f)	
		if qif:
			for tno, qt in enumerate(qif.get_transactions(True)[0]):
				memo_str=""
				if qt.memo:
					memo_str=qt.memo.strip().replace('/','.')
				meta=new_metadata(file.name, tno)
				if qt.category:
					# remove invalid chars, capitalize
					clean_category=qt.category
					for c in quicken_category_remove:
						clean_category= clean_category.replace(c,"")
					cat_toks=clean_category.split(":")
					cap_cats=[]
					for ct in cat_toks: # Capitalize First LetterInWords
						cap_cats.append(ct[0].upper()+ct[1:]) 
					meta['category']=":".join(cap_cats)
				num_str=""
				if qt.num:
					num_str=qt.num.strip()
				if len(num_str) > 0:
					num_str=" "+num_str
				payee_str=""
				check_str=""
				if qt.payee:
					payee_str=qt.payee.strip().replace('/','.')
					if "CHECK" in qt.payee.upper():
						check_str="Check"
				narration_str=" / ".join([payee_str,memo_str,check_str+num_str])
				tn=Transaction(
					meta=meta,
					date=dt.date(qt.date),
					flag="*",
					payee=payee_str,
					narration=narration_str,
					tags=EMPTY_SET,
					links=EMPTY_SET,
					postings=[],
				)
				tn.postings.append(
					Posting(
						account=self.account_name,
						units=Amount(qt.amount,self.currency),
						cost=None,
						price=None,
						flag=None,
						meta={},
					)
				)
				entries.append(tn)
		else:
			sys.stderr.write("Unable to open or parse {0}".format(file.name))	
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
#		ds=dt.date(dt.fromtimestamp(os.path.getmtime(file.name))).isoformat()
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
