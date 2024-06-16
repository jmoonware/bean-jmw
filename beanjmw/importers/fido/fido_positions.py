# custom importer to load Fildelity CSV brokerage positions

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType, Balance
from beancount.core.number import MISSING
from beanjmw.importers import importer_shared

import os,sys, re

from datetime import datetime as dt

fido_column_map = {
"Account Number":"account",
"Account Name":"account_name",
"Symbol":"symbol",
"Description":"description",
"Quantity":"quantity",
"Last Price":"price",
"Last Price Change":"price_change",
"Current Value":"amount",
"Today's Gain/Loss Dollar":"gain_today",
"Today's Gain/Loss Percent":"gain_perc",
"Total Gain/Loss Dollar":"gain_total",
"Total Gain/Loss Percent":"gain_total_perc",
"Percent Of Account":"account_percent",
"Cost Basis Total":"cost_basis_total",
"Average Cost Basis":"cost_basis_average",
"Type":"type",
}

fido_cols = list(fido_column_map.keys())

from collections import namedtuple
FidoRow = namedtuple('FidoRow',list(fido_column_map.values()))

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
		self.default_payee = "Fido CSV"
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
			search_tok = list(fido_column_map.keys())[0]
			while ln < len(head_lines) and search_tok not in head_lines[ln].split(',')[0]:
				ln+=1
			if ln == len(head_lines): # still no love...
				return False
			for l in head_lines[ln+1:]:
				toks=l.split(',')
				if len(toks) > 1:
					fa=self.unquote(toks[0])
					if fa[len(fa)-4:]==self.acct_tail:
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
		import_table, balance_date = self.create_table(file.name)
		utr = self.map_universal_table(import_table, balance_date)
		entries = self.get_balances(utr)
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

	def get_balances(self,unrs):
		""" Creates balance statements from unirows
			Args: stmt = list of unirows
		"""
		balances=[]
		for uni in unrs:
			if uni.action=='balance': 
				if uni.quantity==None:
					acct = self.account_name + ":Cash"
					amt = Amount(uni.amount,self.currency)
				else:
					acct = ":".join([self.account_name,uni.symbol])
					amt = Amount(uni.quantity,uni.symbol)
				nbal = Balance(
					meta=new_metadata("FidoPosition", 0),
					date = uni.date,
					account = acct,
					amount = amt, 
					tolerance = None,
					diff_amount = None,
				)
				balances.append(nbal)
			else:
				sys.stderr.write("Unexpected action for Fido Positions {0}\n".format(uni))

		return(balances)

	def map_universal_table(self,table, balance_date):
		""" Translate bespoke input table into universal single-line records

		Args:
			table, with columns defined at top
		Returns:
			list of UniRow records, mapped from input table
		"""
		unirows=[]
		# fido-specific logic to convert to universal rows for balance
		for tr in table:
			urd = importer_shared.map_to_dict(fido_column_map.values(), tr)
			# FIDO specific: replace with datetime.date value 
			urd['date'] = balance_date
			urd['action']='balance'
			# clean up dict values
			importer_shared.decimalify(urd)
			# make named tuple from final dict
			unirows.append(importer_shared.UniRow(**urd))

		return(unirows)

	# remove single quotes if present...
	def unquote(self,s):
		unquote=s
		if s and len(s) > 0 and s[0]=="'" and s[-1]=="'":
			unquote = s[1:-1]
		return unquote

	def create_table(self,filename):
		""" Returns a list of (mostly) unparsed string tokens
	        each item in the table is a list of tokens exactly 
			len(fido_column_map) long
			Arguments:
				lines: list of raw lines from csv file
		"""
		table=[]
		balance_date = dt.date(dt.now())

		try:
			with open(filename,'r') as f:
				lines=f.readlines()
		except:
			sys.stderr.write("Unable to open or parse {0}".format(filename))
			return(table, balance_date)

		# get the date for all balances
		for l in lines:
			if "Date downloaded" in l:
				toks = l.split(' ')
				if '/' in toks[2]:
					balance_date = dt.date(dt.strptime(toks[2].strip(),'%m/%d/%Y'))
				elif '-' in toks[2]:
					balance_date = dt.date(dt.strptime(toks[2].strip(),'%b-%d-%Y'))
				else:
					raise(ValueError("Yet another date format: "+toks[2]))
				
		nl=0
		search_tok=list(fido_column_map.keys())[0]
		while nl < len(lines) and search_tok not in lines[nl].split(',')[0]:
			nl+=1

		# make sure the columns haven't changed... 
		if nl >= len(lines): # didn't find line with first column tok
			is_fido=False
		else:
			is_fido=True
			cols=[self.unquote(c.strip()) for c in lines[nl].split(',')]
			for c,fc in zip(cols,fido_cols):
				if not fc in c:
					is_fido=False
					break
		if not is_fido or len(cols)!=len(fido_cols):
			sys.stderr.write("Bad format {0} (len={1}), should be {2} (len={3})".format(cols,len(cols),fido_cols,len(fido_cols)))
			return(table, balance_date)
	
		# it's got the right columns, now extract the data	
		for l in lines[nl+1:]:
			ctoks=[self.unquote(c.strip()) for c in l.split(',')]
			if len(ctoks) >= len(fido_cols):
				if ctoks[0][len(ctoks[0])-4:]==self.acct_tail:
					# remove double quotes
					sctoks=[c.strip().replace('"','').replace('$','') for c in ctoks]
					table.append(sctoks[:len(fido_column_map)])

		return(table, balance_date)
