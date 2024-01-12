# custom importer to load simple CSV  account history

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType
from beancount.core.number import MISSING

import os,sys, re

from datetime import datetime as dt

default_open_date='2000-01-01'

# column delimiter
splitchar=',' 

# need at least these columns assigned
required_fields = ['date','amount','description']

# map from column names to tuple fields - must include required_fields above 
default_csv_map={"Trans. Date":'date',"Post Date":'postdate',"Description":'description',"Amount":'amount',"Category":'category'}

from collections import namedtuple

class Importer(ImporterProtocol):
	def __init__(self,account_name,currency='USD',account_number=None,csv_map=None):
		self.account_name=account_name
		if account_number:
			self.acct_tail=account_number[-4:]
		else:
			self.acct_tok=self.account_name.split(':')[-1]
			self.acct_tail=self.acct_tok[len(self.acct_tok)-4:] 
		self.default_payee=account_name.split(":")[-1]+self.acct_tail
		self.currency=currency
		# for simple cash accounts, usually only one default account
		# the yaml assigner will use the description later for other accounts
		self.account_currency={account_name:currency}
		if not csv_map:
			csv_map=default_csv_map
		# check for required fields	 
		for r in required_fields:
			if not r in csv_map.values():
				sys.stderr.write("Missing required {0} field\n".format(r))

		self.csv_row_fields = list(csv_map.values())
		self.csv_cols = list(csv_map.keys())
		self.CsvRow = namedtuple('CsvRow',self.csv_row_fields)
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
			# just use the file name for now
			if self.acct_tail in file.name:
				found=True
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
		try:
			with open(file.name,'r') as f:
				lines=f.readlines()
		except:
			sys.stderr.write("Unable to open or parse {0}".format(file.name))
			return(entries)
		import_table=self.create_table(lines)
		entries = self.get_transactions(import_table)

		# add open directives; some may be removed in dedup
		open_date=dt.date(dt.fromisoformat(default_open_date))
		open_entries=[Open({'lineno':0,'filename':self.account_name},open_date,a,[c],Booking("FIFO")) for a,c in self.account_currency.items()]	
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

	def get_transactions(self,table):
		entries=[]
		for fr in map(self.CsvRow._make,table): 
			# meta={"lineno":0,"filename":self.account_name}
			meta=new_metadata(self.account_name, 0)
			narration_str=fr.description.strip().replace('/',' ')
			tn=Transaction(
				meta=meta,
				date=dt.date(dt.strptime(fr.date,'%m/%d/%Y')),
				flag="*",
				payee=self.default_payee,
				narration=narration_str,
				tags=EMPTY_SET,
				links=EMPTY_SET,
				postings=self.generate_investment_postings(fr),
			)
			entries.append(tn)

		return(entries)

	def generate_investment_postings(self,fr):
		postings=[]

		amt = Decimal('0')
		if len(fr.amount)>0:
			amt = Decimal(fr.amount)
		postings.append(
			Posting(
				account = self.account_name,
				units=Amount(-amt,self.currency),
				cost=None,
				price=None,
				flag=None,
				meta={}
			)
		)
	
		return(postings)


	def create_table(self,lines):
		""" Returns a list of (mostly) unparsed string tokens
	        each item in the table is a list of tokens exactly 
			len(csv_row_fields) long
			Arguments:
				lines: list of raw lines from csv file
		"""
		table=[]
		nl=0
		while nl < len(lines):
			if self.csv_cols[0] in lines[nl]:
				break
			nl+=1

		# make sure the columns haven't changed... 
		is_csv=True
		cols=[c.strip() for c in lines[nl].strip(', ').split(',')]
		for c,fc in zip(cols,self.csv_cols):
			if len(c.strip())!=0 and c!=fc:
				is_csv=False
				break
		if not is_csv: # or len(cols)!=len(csv_cols):
			sys.stderr.write("Bad format {0}".format(cols))
			return(table)
	
		# it's got the right columns, now extract the data	
		for il,l in enumerate(lines[nl+1:]):
			clean_l=l.strip(', \n')
			ctoks=clean_l.split(splitchar)
			if len(ctoks) > len(self.csv_cols) and '"' in l: # splitter in quote! 
				dq=re.search('"(.*?)"',clean_l)
				if dq:
					old_tok=clean_l[dq.span()[0]:dq.span()[1]]
					new_tok=clean_l[dq.span()[0]:dq.span()[1]].replace(splitchar,' ')
					clean_l=clean_l.replace(old_tok,new_tok)
			ctoks=clean_l.split(splitchar)
			if len(ctoks)==len(self.csv_cols):
				# remove double quotes
				sctoks=[c.strip().replace('"','') for c in ctoks]
				table.append(sctoks[:len(self.csv_row_fields)])
			else:
				sys.stderr.write("Bad format line {0}: {1} {2}".format(il,clean_l,ctoks))
				
		return(table)
