# custom importer to load Etrade CSV brokerage account history

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType
from beancount.core.number import MISSING
import beanjmw.importers.importer_shared as impshare

import os,sys, re

from datetime import datetime as dt

# remove these chars as Beancount accounts can't have them
quicken_category_remove=[' ','\'','&','-','+','.']

# ETrade has a more descriptive TransactionType col that doesn't easily map
# to QIF actions...
transaction_types={
'Adjustment', # at zero cost
'Bought',
'Direct Debit', 
'Dividend', # could be LT, ST, etc.
'Fee',
'Interest',
'Other', # appear to be cancelling pairs of share sales/buys
'Reorganization', # mutual funds changing symbol
'Sold',
'Transfer',
'Wire',
}

action_map={
'Adjustment':'Other', # at zero cost
'Bought':'Buy',
'Direct Debit':'Debit', 
'Dividend':'Div', # could be LT, ST, etc.
'Fee':'MiscExp',
'Interest':'IntInc',
'Other':'Other', # appear to be cancelling pairs of share sales/buys
'Reorganization':'Merger', # mutual funds changing symbol
'Sold':'Sell',
'Transfer':'Xout',
'Wire':'Xout',
}

# in the Description field, at least since 2021 
# Does not work for previous years!
investment_actions={
'L/T CAPITAL GAIN':'CGLong',
'S/T CAPITAL GAIN':'CGShort',
'REINVEST':'Div',
}

default_open_date='2000-01-01'

etrade_cols = [
'TransactionDate','TransactionType','SecurityType','Symbol','Quantity','Amount','Price','Commission','Description',
]

# map to universal row
etrade_map_cols = {
'TransactionDate':'date',
'TransactionType':'action',
'SecurityType':'type',
'Symbol':'symbol',
'Quantity':'quantity',
'Amount':'amount',
'Price':'price',
'Commission':'commission',
'Description':'description',
}

from collections import namedtuple
EtradeRow = namedtuple('EtradeRow',etrade_cols)

class Importer(ImporterProtocol):
	def __init__(self,account_name,currency='USD',account_number=None):
		self.account_name=account_name
		if not account_number:
			acct_tok=self.account_name.split(':')[-1]
			self.acct_tail=acct_tok[-4:] 
		else:
			self.acct_tail=account_number[-4:]
		self.currency=currency
		self.account_currency={} # added as discovered
		self.default_payee="Etrade CSV"
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
			while ln < len(head_lines):
				if 'For Account' in head_lines[ln]:
					toks=head_lines[ln].split(',')
					if len(toks) > 1:
						fa=toks[1]
						if fa[len(fa)-4:]==self.acct_tail:
							found=True
							break
				ln+=1
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
		try:
			with open(file.name,'r') as f:
				lines=f.readlines()
		except:
			sys.stderr.write("Unable to open or parse {0}".format(file.name))
			return([])
		# read Etrade-specific format
		import_table=self.create_table(lines)

		# map to universal rows
		transactions = self.map_universal_transactions(import_table)
		# turn into Beancount transactions
		entries = impshare.get_transactions(transactions, self.account_name, self.default_payee, self.currency, self.account_currency)
		return(entries)
		
		entries = self.get_transactions(import_table)

		# add open directives; some may be removed in dedup
		open_date=dt.date(dt.fromisoformat(default_open_date))
		open_entries=[Open({'lineno':0,'filename':self.account_name},open_date,a,c,Booking("FIFO")) for a,c in self.account_currency.items()]	
		return(open_entries + entries)

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

	def map_actions(self,urd):
		if urd['action'] in action_map:
			urd['action']=action_map[urd['action']]
		else: # unsure what we should do here so warn
			sys.stderr.write("map_actions: Unknown inv action: {0} in {1}\n".format(urd['action'],urd))
		# special Etrade logic: two records for dividends
		# First has a zero-quantity marked as REINV, LT, or ST Cap Gain
		# (although the marking in description is only since 2021 I think)
		# Second is the (possible) actual buying of the security
		if urd['action']=='Div':
			if urd['quantity']!=0:
				urd['action']='Buy' # reinvesting of Div, CGLong or CGShort
				# Etrade supplies 0 as the price! Need to recalculate
				urd['price']=abs(round(urd['amount']/urd['quantity'],2))
			else: # zero quantity cash increase
				for key in investment_actions:
					if key in urd['description']:
						urd['action']=investment_actions[key]	
		return

	def map_universal_transactions(self,rows):
		""" Map raw rows to universal rows, Etrade specific
			Args:
				rows: rows from loaded table

			Returns: a list of universal rows
		"""

		uentries = []

		for row in rows:
			urd = impshare.map_to_dict(etrade_map_cols.values(), row)
			# Etrade specific date
			urd['date']=dt.date(dt.strptime(urd['date'],'%m/%d/%y'))
			impshare.build_narration(urd)
			#  make sure we have a sensible symbol
			if len(urd['symbol']) == 0 or '#' in urd['symbol']:
				urd['symbol']=self.currency 
			impshare.decimalify(urd)
			self.map_actions(urd)
			uentries.append(impshare.UniRow(**urd))

		return(uentries)

	def create_table(self,lines):
		""" Returns a list of (mostly) unparsed string tokens
	        each item in the table is a list of tokens exactly 
			len(etrade_cols) long
			Arguments:
				lines: list of raw lines from csv file
		"""
		table=[]
		nl=0
		while nl < len(lines):
			if "TransactionDate" in lines[nl]:
				break
			nl+=1

		# make sure the columns haven't changed... 
		is_etrade=True
		cols=[c.strip() for c in lines[nl].split(',')]
		for c,fc in zip(cols,etrade_cols):
			if c!=fc:
				is_etrade=False
				break
		if not is_etrade or len(cols)!=len(etrade_cols):
			sys.stderr.write("Bad format {0}".format(cols))
			return(table)
	
		# it's got the right columns, now extract the data	
		for l in lines[nl+1:]:
			ctoks=l.split(',')
			if len(ctoks) > 0 and len(ctoks[0])==0: # filter blank date
				continue
			if len(ctoks) >= len(etrade_cols):
				table.append([c.strip() for c in ctoks[:len(etrade_cols)]])

		return(table)
