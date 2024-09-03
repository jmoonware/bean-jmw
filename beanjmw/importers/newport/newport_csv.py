# custom importer to load Newport CSV brokerage account history
# This uses files scraped from the PDF reports (with some hand-editing)
# This isn't really useful as a general Newport importer (use the ofx files) 

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType
from beancount.core.number import MISSING
from beanjmw.importers.importer_shared import unquote

import beanjmw.importers.importer_shared as impshare

import os,sys, re

from datetime import datetime as dt

action_map={
'BUY':'Buy',
'SELL':'Sell',
}

default_open_date='2000-01-01'

newport_map = {
'Date':'date',
'Type':'type',
'Source':'NA',
'Ticker':'NA',
'CUSIP':'NA',
'Investment':'symbol',
'Quantity':'quantity',
'Price':'price',
'Total':'amount',
'Activity':'description',
}

newport_cols = list(newport_map.keys())

# if key is in symbol, then remap symbol to value
newport_symbol_map = {'JANUSHENDERSONVITENT':'JAAGX'}

class Importer(ImporterProtocol):
	def __init__(self,account_name,currency='USD',account_number=None):
		self.account_name=account_name
		self.acct_tok=self.account_name.split(':')[-1]
		if account_number:
			self.acct_number = account_number
			self.acct_tail=self.acct_number[-4:] 
		else: # take from name
			self.acct_tail=self.acct_tok[-4:] 
		self.currency=currency
		self.account_currency={} # added as discovered
		self.default_payee="Newport CSV"
		super().__init__()

	def identify(self, file):
		"""Return true if this importer matches the given file.
			Args:
				file: A cache.FileMemo instance.
			Returns:
				A boolean, true if this importer can handle this file.
		"""
		if os.path.splitext(file.name)[1].upper()=='.CSV':
			# assumes account # comes up in first head() lines...
			head_lines=file.head(num_bytes=100000).split('\n')
			found=False
			ln=0
			if self.acct_tail in file.name:
				while ln < len(head_lines):
					toks=head_lines[ln].split(',')
					if newport_cols[0] in unquote(toks[0]): # found first header
						found=True
						break
					ln+=1
			return found
		else:
			 return False
		
	def extract(self, file, existing_entries=None,account_number=None):
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
			with open(file.name,'r') as f:
				lines=f.readlines()
		except:
			sys.stderr.write("Unable to open or parse {0}\n".format(file.name))
			return(entries)
		import_table=self.create_table(lines)
		uentries = self.map_universal_table(import_table)
		entries = impshare.get_transactions(uentries, self.account_name, self.default_payee, self.currency, self.account_currency)
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

	def map_universal_table(self,table):
		uentries=[]
		for tr in table:
			# universal row dict
			urd = impshare.UniRow()._asdict()
			for key,val in zip(newport_map.values(),tr):
				if key in urd:
					urd[key]=val
			urd['date'] = dt.date(dt.strptime(urd['date'],'%m/%d/%Y'))
			urd['narration']=" / ".join([urd['description'],urd['type']])
			if urd['type'] in action_map:
				urd['action']=action_map[urd['type']]
			else:
				sys.stderr.write("Newport CSV map_universal_table: Unknown action {0}\n".format(urd['type']))
			# fix up ugly investment ticker names...
			urd['symbol']=urd['symbol'].upper().replace(' ','').replace('"','')[:20]

			# fix up parentheses in negative amounts...
			if '($' in urd['amount']:
				urd['amount']=urd['amount'].replace('($','-').replace(')','')
			if '$' in urd['amount']:
				urd['amount']=urd['amount'].replace('$','')

			impshare.decimalify(urd)
			uentries.append(impshare.UniRow(**urd))
		return(uentries)

	def create_table(self,lines):
		""" Returns a list of (mostly) unparsed string tokens
	        each item in the table is a list of tokens exactly 
			len(newport_cols) long
			Arguments:
				lines: list of raw lines from csv file
		"""
		table=[]
		nl=0
		while nl < len(lines): # skip blanks, look for first col header
			if newport_cols[0] in lines[nl]:
				break
			nl+=1

		# make sure the columns haven't changed... 
		is_newport=True
		cols=[unquote(c.strip().replace('\'','').replace('"','')) for c in lines[nl].split(',')]
		for c,fc in zip(cols,newport_cols):
			if c!=fc:
				is_newport=False
				break
		if not is_newport or len(cols)!=len(newport_cols):
			sys.stderr.write("Bad format {0}".format(cols))
			return(table)
	
		# it's got the right columns, now extract the data	
		for l in lines[nl+1:]:
			ctoks=[unquote(ct) for ct in l.split(',')]
			if len(ctoks) > 0 and len(ctoks[0])==0: # filter blank date
				continue
			if len(ctoks) >= len(newport_cols):
				table.append([c.strip().replace('\'','') for c in ctoks[:len(newport_cols)]])

		return(table)
