# custom importer to load Etrade/MorganStanley CSV brokerage positions

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType, Balance
from beancount.core.number import MISSING
from beanjmw.importers import importer_shared

import os,sys, re

from datetime import datetime as dt

etrade_column_map = {
"Symbol":"symbol",
"Last Price $":"price",
"Change $":"price_change",
"Change %":"price_change_perc",
"Quantity":"quantity",
"Price Paid $":"price_paid",
"Day's Gain $":"days_gain",
"Total Gain $":"total_gain",
"Total Gain %":"total_gain_perc",
"Value $":"value",
}

etrade_cols = list(etrade_column_map.keys())

from collections import namedtuple
EtradeRow = namedtuple('EtradeRow',list(etrade_column_map.values()))

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
		self.default_payee = "Etrade CSV"
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
			# Acct tail should be in first tok of line split 
	#		breakpoint()
			while ln < len(head_lines):
				ftok = head_lines[ln].split(',')[0].split('-')[-1]
				if ftok[:4]==self.acct_tail:
					found=True
					break
				ln+=1
			if ln >= len(head_lines): # still no love...
				return False
			
			if not found:
				return False

			# now look for exact header column match further into file
			tok_pos = 1
			search_tok = list(etrade_column_map.keys())[tok_pos]
			# FIXME: find SECOND col token match
			while ln < len(head_lines):
				stoks = head_lines[ln].split(',')
				if len(stoks) > tok_pos and search_tok in stoks[tok_pos]:
					break 
				ln+=1
			if ln >= len(head_lines): # still no love...
				return False

			match_toks = head_lines[ln].strip().split(',')
			matched=0
			for tok,col_tok in zip(match_toks,etrade_cols):
				clean_tok=tok.strip()
				if clean_tok==col_tok:
					matched+=1
			if matched==len(etrade_cols):
				return True 
		else:
			 return False
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
				if uni.quantity==None and uni.symbol==self.currency: 
					acct = self.account_name + ":Cash"
					amt = Amount(uni.amount,self.currency)
				else:
					symbol = uni.symbol
					acct = ":".join([self.account_name,symbol])
					if uni.quantity!=None:
						amt = Amount(uni.quantity,symbol)
					else:
						amt = Amount(uni.amount,symbol)
				nbal = Balance(
					meta=new_metadata("EtradePosition", 0),
					date = uni.date,
					account = acct,
					amount = amt, 
					tolerance = None,
					diff_amount = None,
				)
				balances.append(nbal)
			else:
				sys.stderr.write("Unexpected action for Etrade Positions {0}\n".format(uni))

		return(balances)

	def map_universal_table(self,table, balance_date):
		""" Translate bespoke input table into universal single-line records

		Args:
			table, with columns defined at top
		Returns:
			list of UniRow records, mapped from input table
		"""
		unirows=[]
		# etrade-specific logic to convert to universal rows for balance
		for tr in table:
			urd = importer_shared.map_to_dict(etrade_column_map.values(), tr)
			# FIDO specific: replace with datetime.date value 
			urd['date'] = balance_date
			urd['action']='balance'
			# FIDO sometimes tacks on ** to the symbols, gahh
			if '**' in urd['symbol']:
				urd['symbol'] = urd['symbol'].replace('**','')
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
			len(etrade_column_map) long
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
			if "Generated at" in l:
				dtoks = l.split(' ')
				if len(dtoks) >= 5:
					balance_date = dt.date(dt.strptime(' '.join(dtoks[2:5]),"%b %d %Y"))
				else:
					raise(ValueError("Yet another date format: "+l))
				
		nl=0
		tok_pos=1
		search_tok=list(etrade_column_map.keys())[tok_pos]
		while nl < len(lines):
			stoks =lines[nl].split(',')
			if len(stoks) > tok_pos and search_tok in stoks[tok_pos]:
				break
			nl+=1

		# make sure the columns haven't changed... 
		if nl >= len(lines): # didn't find line with first column tok
			is_etrade=False
		else:
			is_etrade=True
			cols=[self.unquote(c.strip()) for c in lines[nl].split(',')]
			for c,fc in zip(cols,etrade_cols):
				if not fc in c:
					is_etrade=False
					break
		if not is_etrade or len(cols)!=len(etrade_cols):
			sys.stderr.write("Bad format {0} (len={1}), should be {2} (len={3})".format(cols,len(cols),etrade_cols,len(etrade_cols)))
			return(table, balance_date)
	
		# it's got the right columns, now extract the data	
		for l in lines[nl+1:]:
			ctoks=[self.unquote(c.strip()) for c in l.split(',')]
			if len(ctoks) >= len(etrade_cols):
				# remove double quotes
				sctoks=[c.strip().replace('"','').replace('$','') for c in ctoks]
				table.append(sctoks[:len(etrade_column_map)])

		return(table, balance_date)
