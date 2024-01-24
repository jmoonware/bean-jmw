# custom importer to load Vanguard CSV brokerage account history
# This uses files scraped from the PDF reports (with some hand-editing)
# is isn't really useful as a general Vanguard importer (use the ofx files) 

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType
from beancount.core.number import MISSING

import os,sys, re

from datetime import datetime as dt

# remove these chars as Beancount accounts can't have them
quicken_category_remove=[' ','\'','&','-','+','.']

transaction_types={
'Buy',
'Dividend',
'Conversion',
'Reinvestment',
'Transfer (incoming)',
'Transfer',
'Capital gain (LT)',
'Capital gain (ST)',
'Reinvestment (LT gain)',
'Reinvestment (ST gain)',
}

transaction_acct={
'Dividend':'Div', 
'Reinvestment':'Div', 
'Reinvestment (LT gain)':'CGLong',
'Reinvestment (ST gain)':'CGShort',
}

skip_zeros=[
'Transfer (incoming)',
'Dividend',
'Capital gain (LT)',
'Capital gain (ST)',
]

default_open_date='2000-01-01'

vanguard_cols = [
'SettlementDate','TradeDate','Symbol','Description','TransactionType','Quantity','Price','Amount',
]

from collections import namedtuple
VanguardRow = namedtuple('VanguardRow',vanguard_cols)

class Importer(ImporterProtocol):
	def __init__(self,account_name,currency='USD',account_number=None):
		self.account_name=account_name
		self.acct_tok=self.account_name.split(':')[-1]
		if account_number:
			self.acct_tail=self.acct_number[-4:] 
		else: # take from name
			self.acct_tail=self.acct_tok[-4:] 
		self.currency=currency
		self.account_currency={} # added as discovered
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
					if vanguard_cols[0] in toks[0]: # found first header
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
		open_entries=[Open({'lineno':0,'filename':self.account_name},open_date,a,["USD",c],Booking("FIFO")) for a,c in self.account_currency.items()]	
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
		for fr in map(VanguardRow._make,table): 
			meta=new_metadata(self.account_name, 0)
			# KLUDGE: Fix amounts without decimal point
			nfr=fr._replace() # make a copy
			if not '.' in fr.Amount:
				namt = fr.Amount+".00"
				nfr = fr._replace(Amount=namt)
			# Note: this assumes there is another record for reinvesting
			if fr.TransactionType=='Transfer' or (fr.TransactionType in skip_zeros and len(fr.Price)==0):
				sys.stderr.write("Skipping {0} {1} {2}\n".format(fr.TransactionType,fr.TradeDate,fr.Symbol))
				continue
			# filter out transactions that are to be ignored
			if "IGNORE" in fr.Description:
				continue
			narration_str=" / ".join([fr.Description,fr.TransactionType])
			tn=Transaction(
				meta=meta,
				date=dt.date(dt.strptime(nfr.TradeDate,'%m/%d/%Y')),
				flag="*",
				payee="Vanguard csv",
				narration=narration_str,
				tags=EMPTY_SET,
				links=EMPTY_SET,
				postings=self.generate_investment_postings(nfr),
			)
			entries.append(tn)

		return(entries)

	def generate_investment_postings(self,fr):
		postings=[]

		# try to find investment action
		# switch to use QIF format names
		# TODO: Re-use code in qif importer
		vanguard_action=None
		if fr.TransactionType in transaction_types:
			vanguard_action=fr.TransactionType

		# unsure what we should do here so bail
		if not vanguard_action:
			sys.stderr.write("Unknown inv action: {0} in {1}\n".format(fr.TransactionType,fr))
			return(postings)
	
		# set defaults for two generic postings (p0, p1)
		symbol=self.currency # default to this
		if len(fr.Symbol) > 0 and not '#' in fr.Symbol:
			symbol = fr.Symbol
		sec_currency=symbol
		sec_account=symbol
		acct = ":".join([self.account_name, sec_account])
		# open account with this currency
		self.account_currency[acct]=sec_currency
		qty = Decimal('0')
		if len(fr.Quantity)>0:
			qty = Decimal(fr.Quantity)
		postings.append(
			Posting(
				account = self.account_name,
				units=Amount(qty,sec_currency),
				cost=None,
				price=None,
				flag=None,
				meta={}
			)
		)
		postings.append(
			Posting(
				account = self.account_name + ":Cash",
				units=Amount(-qty,sec_currency),
				cost=None,
				price=None,
				flag=None,
				meta={}
			)
		)
		# for convenience
		p0=postings[0]
		p1=postings[1] 
	
		# deal with each type of investment action:
		if vanguard_action in ['Buy','Conversion']:
			acct = ":".join([self.account_name, sec_account])
			meta=new_metadata(acct, 0)
			amt=Decimal(0)
			if len(fr.Amount)>0:
				amt=Decimal(fr.Amount)
			qty=Decimal(0.000000001)
			if fr.Quantity:
				qty=Decimal(fr.Quantity)
			prc=Decimal('0.00')
			if len(fr.Price)>0:
				prc=Decimal(fr.Price)
				tprc=abs(amt/qty)
				if abs(qty*(tprc-prc)) > 0.0025: # exceeds tolerance
					meta["rounding"]="Price was {0}".format(prc)
					prc=tprc
			postings[0]=p0._replace(
				account = acct,
				units=Amount(Decimal(fr.Quantity),sec_currency),
				price = Amount(prc,self.currency),
				meta = meta,
			)
			aname='Cash'
			postings[1]=p1._replace(
				account = ":".join([self.account_name,aname]),
				units = Amount(amt,self.currency)
			)
		elif vanguard_action=='Sold': 
			commission=Decimal(0)
			if len(fr.Commission)>0:
				commission=Decimal(fr.Commission)
				postings.append(
					Posting(
						account = self.account_name.replace('Assets','Expenses') + ":Commission",
						units=Amount(commission,self.currency),
						cost=None,
						price=None,
						flag=None,
						meta={}
					)
				)
			total_cost=commission
			if len(fr.Amount)>0:
				total_cost=Decimal(fr.Amount)+commission
			prc=Decimal(0)
			if len(fr.Price)>0:
				prc=Decimal(fr.Price)
			postings[0]=p0._replace(
				account = ":".join([self.account_name, sec_account]),
				units=Amount(Decimal(fr.Quantity),sec_currency),
		#		cost=None, # let Beancount FIFO booking rule take care
				price = Amount(prc,self.currency),
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(total_cost-commission,self.currency)
			)
			# interpolated posting
			postings.append(
				Posting(
					account = self.account_name.replace('Assets','Income')+":PnL",
					units = NoneType(),
					cost = None,
					price = None,
					flag = None,
					meta=None,
				)
			)
		elif vanguard_action in transaction_acct:
			from_acct=transaction_acct[vanguard_action]
			price_amt = None
			account = ":".join([self.account_name,sec_account])
			units=Amount(Decimal(fr.Quantity),sec_currency)
			if fr.Quantity and len(fr.Quantity) > 0:
				prc = abs(Decimal(fr.Amount)/Decimal(fr.Quantity))
				price_amt = Amount(prc,self.currency)
			postings[0]=p0._replace(
				account = account,
				units = units,
 				price = price_amt,
			)
			postings[1]=p1._replace(
				account = ":".join([self.account_name.replace("Assets","Income"),sec_currency,from_acct]),
				units = Amount(Decimal(fr.Amount),self.currency)
			)
		elif vanguard_action == 'Interest':
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Income'),"IntInc"]),
				units=Amount(-Decimal(fr.Amount),self.currency)
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(Decimal(fr.Amount),self.currency)
			)
		elif vanguard_action == 'Fee':
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Expenses'),vanguard_action]),
				units=Amount(-Decimal(fr.Amount),self.currency)
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(Decimal(fr.Amount),self.currency)
			)
		# Adjustment just removes or adds shares at 0 cost - basis 
		# needs to be entered manually (maybe a way to get this?)
		elif vanguard_action in ['Adjustment','Other','Reorganization']:
			postings[0]=p0._replace(
				account = ":".join([self.account_name,sec_currency]),
				units=Amount(Decimal(fr.Quantity),sec_currency),
				price = Amount(Decimal(0),self.currency),
			)
			meta=new_metadata(self.account_name, 0)
			meta["fixme"] = "Posting may need cost basis"
			postings[1]=p1._replace(
				account = ":".join([self.account_name, "Adjustment"]),
				units=Amount(Decimal(0),self.currency),
				price = Amount(Decimal(0),self.currency),
				meta = meta,
			)
		else:
			sys.stderr.write("Unknown investment action {0}\n".format(vanguard_action))
	
		return(postings)


	def create_table(self,lines):
		""" Returns a list of (mostly) unparsed string tokens
	        each item in the table is a list of tokens exactly 
			len(vanguard_cols) long
			Arguments:
				lines: list of raw lines from csv file
		"""
		table=[]
		nl=0
		while nl < len(lines): # skip blanks, look for first col header
			if vanguard_cols[0] in lines[nl]:
				break
			nl+=1

		# make sure the columns haven't changed... 
		is_vanguard=True
		cols=[c.strip().replace('\'','').replace('"','') for c in lines[nl].split(',')]
		for c,fc in zip(cols,vanguard_cols):
			if c!=fc:
				is_vanguard=False
				break
		if not is_vanguard or len(cols)!=len(vanguard_cols):
			sys.stderr.write("Bad format {0}".format(cols))
			return(table)
	
		# it's got the right columns, now extract the data	
		for l in lines[nl+1:]:
			ctoks=l.split(',')
			if len(ctoks) > 0 and len(ctoks[0])==0: # filter blank date
				continue
			if len(ctoks) >= len(vanguard_cols):
				table.append([c.strip().replace('\'','') for c in ctoks[:len(vanguard_cols)]])

		return(table)
