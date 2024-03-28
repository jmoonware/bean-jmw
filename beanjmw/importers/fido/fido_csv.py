# custom importer to load Fildelity CSV brokerage account history

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType
from beancount.core.number import MISSING
from beanjmw.importers import importer_shared

import os,sys, re

from datetime import datetime as dt

# all possible actions for Fido investments, map to universal
investment_actions={
'BUY':'Buy', 
'YOU BOUGHT':'Buy', 
'YOU SOLD':'Sell',
'LONG-TERM CAP GAIN':'CGLong',
'SHORT-TERM CAP GAIN':'CGShort',
'DIVIDEND RECEIVED':'Div',
'INTEREST':'IntInc',
'REINVESTMENT':'Buy',
'ELECTRONIC FUNDS TRANSFER':'Xout',
'DIRECT DEBIT':'Xout',
'MERGER ':'Merger',
'DISTRIBUTION':'ShrsIn',
}

default_open_date='2000-01-01'

# Downloaded column names: Universal Names
fido_column_map = {
'Run Date':'date', 
'Account':'account', 
'Action':'action', 
'Symbol':'symbol', 
'Description':'description', 
'Type':'type', 
'Quantity':'quantity', 
'Price ($)':'price', 
'Commission ($)':'commission', 
'Fees ($)':'fees', 
'Accrued Interest ($)':'accrued_interest',
'Amount ($)':'amount',
'Settlement Date':'settlement_date',
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
			while ln < len(head_lines) and "Run Date" not in head_lines[ln].split(',')[0]:
				ln+=1
			if ln == len(head_lines): # still no love...
				return False
			for l in head_lines[ln+1:]:
				toks=l.split(',')
				if len(toks) > 1:
					fa=self.unquote(toks[1])
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
		import_table=self.create_table(file.name)
		utr = self.map_universal_table(import_table)
		entries = importer_shared.get_transactions(utr, self.account_name, self.default_payee, self.currency, self.account_currency)


#		entries = self.get_transactions(import_table)

		# add open directives; some may be removed in dedup
#		open_date=dt.date(dt.fromisoformat(default_open_date))
#		open_entries=[Open({'lineno':0,'filename':self.account_name},open_date,a,c,Booking("FIFO")) for a,c in self.account_currency.items()]	
#		return(open_entries + entries)
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

	def map_actions(self,urd):
		# now map action
		fido_action=None
		if "action" in urd:
			for ia in investment_actions:
				if ia in urd["action"].upper(): # found a match
					fido_action=investment_actions[ia]
					break
			# unsure what we should do here so warn
			if not fido_action:
				sys.stderr.write("Unknown inv action: {0} in {1}\n".format(urd["action"],urd))
		# replace with universal action
		urd["action"]=fido_action
		return

	def map_universal_table(self,table):
		""" Translate bespoke input table into universal single-line records

		Args:
			table, with columns defined at top
		Returns:
			list of UniRow records, mapped from input table
		"""
		unirows=[]
		# fido-specific logic to convert to universal rows
		for tr in table:
			urd = importer_shared.map_to_dict(fido_column_map.values(), tr)
			# give us a narration for transaction
			importer_shared.build_narration(urd)
			# FIDO specific: Actual date may be in action!
			if 'action' in urd and 'as of' in urd['action']:
				dm = re.search("[0-9]{2}/[0-9]{2}/[0-9]{4}",urd['action'])
				urd['date']=dm[0]
			# FIDO specific: replace with datetime.date value 
			urd['date'] = dt.date(dt.strptime(urd['date'],'%m/%d/%Y'))
			# FIDO specific: interest income looks like this
			if "CASH" in urd['description']: # special case
				urd['symbol']=self.currency
			# overwrite the "action" column value with universal action
			self.map_actions(urd)
			# clean up dict values
			importer_shared.decimalify(urd)
			# make named tuple from final dict
			unirows.append(importer_shared.UniRow(**urd))

		return(unirows)

	def get_transactions(self,table):
		entries=[]
		for fr in map(FidoRow._make,table): 
			# meta={"lineno":0,"filename":self.account_name}
			meta=new_metadata(self.account_name, 0)
			# KLUDGE: Fix amounts without decimal point
			nfr=fr._replace() # make a copy
			if not '.' in fr.amount:
				namt = fr.amount+".00"
				nfr = fr._replace(amount=namt)
			# KLUDGE: Actual date may be in action!
			if 'as of' in fr.action:
				dm = re.search("[0-9]{2}/[0-9]{2}/[0-9]{4}",fr.action)
				nfr = nfr._replace(date = dm[0])
			narration_str=" / ".join([fr.account,fr.action])
			nfr = nfr._replace(date = dt.date(dt.strptime(nfr.date,'%m/%d/%Y')))
			tn=Transaction(
				meta=meta,
				date=nfr.date,
				flag="*",
				payee="Investment",
				narration=narration_str,
				tags=EMPTY_SET,
				links=EMPTY_SET,
				postings=self.generate_investment_postings(nfr),
			)
			entries.append(tn)

		return(entries)

	def get_trn_date(self,fr):
		trn_date=None
		if hasattr(fr,'date'):
			trn_date=fr.date
		elif hasattr(fr, 'tradeDate'):
		# FIXME: when use tradeDate vs. settleDate?
			trn_date=fr.tradeDate
		return(trn_date)

	def generate_investment_postings(self,fr):
		postings=[]
		trn_date = self.get_trn_date(fr)

		# try to find investment action
		# switch to use QIF format names
		# TODO: Re-use code in qif importer
		fido_action=None
		for ia in investment_actions:
			if ia in fr.action.upper(): # found a match
				fido_action=investment_actions[ia]
				break

		# unsure what we should do here so bail
		if not fido_action:
			sys.stderr.write("Unknown inv action: {0} in {1}\n".format(fr.action,fr))
			return(postings)
	
		# set defaults for two generic postings (p0, p1)
		sec_name=fr.description
		symbol=self.currency # default to this
		if len(fr.symbol) > 0:
			symbol = fr.symbol
		sec_currency=symbol
		sec_account=symbol
		if "CASH" in fr.description: # special case
			sec_account="Cash"
			sec_currency=self.currency
		acct = ":".join([self.account_name, sec_account])
		# open account with this currency
		self.account_currency[acct]=["USD",sec_currency]
		qty = Decimal('0')
		if len(fr.quantity)>0:
			qty = Decimal(fr.quantity)
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
		if fido_action in ['Buy','ShrsIn']:
			acct = ":".join([self.account_name, sec_account])
			meta=new_metadata(acct, 0)
			amt=Decimal(0)
			if len(fr.amount)>0:
				amt=Decimal(fr.amount)
			qty=Decimal(0.000000001)
			if fr.quantity:
				qty=Decimal(fr.quantity)
			prc=Decimal(0)
			if len(fr.price)>0:
				prc=Decimal(fr.price)
				tprc=abs(amt/qty)
				if abs(qty*(tprc-prc)) > 0.0025: # exceeds tolerance
					meta["rounding"]="Price was {0}".format(prc)
					prc=tprc
			if sec_account=='Cash': # KLUDGE FOR FIDO 
				pcost = None
				pprice = None
			else:
				pcost = Cost(prc,self.currency,trn_date,"")
				pprice = Amount(prc,self.currency) 
			postings[0]=p0._replace(
				account = acct,
				units=Amount(Decimal(fr.quantity),sec_currency),
				cost = pcost,
				price = pprice,
				meta = meta,
			)
			aname='Cash'
			# shares in came from a share exchange somewhere else
			if fido_action == 'ShrsIn': # KLUDGE
				aname = 'Transfer'
			postings[1]=p1._replace(
				account = ":".join([self.account_name,aname]),
				units = Amount(-abs(amt),self.currency)
			)
		elif fido_action=='Sell': 
			commission=Decimal(0)
			if len(fr.commission)>0:
				commission=Decimal(fr.commission)
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
			if len(fr.amount)>0:
				total_cost=Decimal(fr.amount)+commission
			prc=Decimal(0)
			if len(fr.price)>0:
				prc=Decimal(fr.price)
			postings[0]=p0._replace(
				account = ":".join([self.account_name, sec_account]),
				units=Amount(Decimal(fr.quantity),sec_currency),
				# let Beancount FIFO booking rule take care
				cost=Cost(None,None,None,None),
				price = Amount(prc,self.currency),
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(total_cost-commission,self.currency)
			)
			# interpolated posting
			# Open account here as we need the Open to have no 
			# currency explicity defined to pass validation
			# the __residual__ is needed to pass validation
			# Rem to add __residual__ to EntryPrinter.META_IGNORE
			interp_acct = ":".join([self.account_name.replace('Assets','Income'),sec_account,"Gains"])
			self.account_currency[interp_acct]=None
			postings.append(
				Posting(
					account = interp_acct,
					units = NoneType(),
					cost = None,
					price = None,
					flag = None,
					meta={'__residual__':True}
				)
			)
		elif fido_action in ['Div','CGShort','CGLong','CGMid']:
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Income'),sec_account,fido_action]),
				units=Amount(-Decimal(fr.amount),self.currency)
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(Decimal(fr.amount),self.currency)
			)
		elif fido_action in ['IntInc','MiscInc']:
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Income'),fido_action]),
				units=Amount(-Decimal(fr.amount),self.currency)
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(Decimal(fr.amount),self.currency)
			)
		elif fido_action in ['MiscExp']:
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Expenses'),fido_action]),
				units=Amount(-Decimal(fr.amount),self.currency)
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(Decimal(fr.amount),self.currency)
			)
		elif fido_action in ['Xin','Xout']: 
			postings[0]=p0._replace(
				account = ":".join([self.account_name,"Cash"]),
				units=Amount(Decimal(fr.amount),self.currency)
			)
			postings[1]=p1._replace(
				account = ":".join([self.account_name, "Transfer"]),
				units=Amount(-Decimal(fr.amount),sec_currency),
			)
		# Merger just removes or adds shares at 0 cost - basis 
		# needs to be entered manually (maybe a way to get this?)
		elif fido_action == 'Merger':
			postings[0]=p0._replace(
				account = ":".join([self.account_name,sec_currency]),
				units=Amount(Decimal(fr.quantity),sec_currency),
				price = Amount(Decimal(0),self.currency),
			)
			meta=new_metadata(self.account_name, 0)
			meta["fixme"] = "Posting needs cost basis"
			postings[1]=p1._replace(
				account = ":".join([self.account_name, "Merger"]),
				units=Amount(Decimal(0),self.currency),
				price = Amount(Decimal(0),self.currency),
				meta = meta,
			)
		elif fido_action=='StkSplit': 
			pass
		# just remove shares - manually fix where they go later!
		# looks like sale for transfer between e.g. share classes 
		elif fido_action=='ShrsOut': 
			# FIXME
			amt = Decimal('0') # Decimal(fr.amount) is empty!
			price = Decimal('0')
			postings[0]=p0._replace(
				account = ":".join([self.account_name, sec_account]),
				units=Amount(-Decimal(fr.quantity),sec_currency),
				cost=Cost(price,self.currency,dt.date(dt.strptime(fr.date,'%m/%d/%Y')),""),
			)
			postings[1]=p1._replace(
				account = self.account_name + ":FIXME",
				units = Amount(amt,self.currency)
			)
		else:
			sys.stderr.write("Unknown investment action {0}\n".format(fido_action))
	
		return(postings)

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
		try:
			with open(filename,'r') as f:
				lines=f.readlines()
		except:
			sys.stderr.write("Unable to open or parse {0}".format(filename))
			return(table)

		nl=0
		while nl < len(lines) and "Run Date" not in lines[nl].split(',')[0]:
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
			return(table)
	
		# it's got the right columns, now extract the data	
		for l in lines[nl+1:]:
			ctoks=[self.unquote(c.strip()) for c in l.split(',')]
			if len(ctoks) >= len(fido_cols):
				if ctoks[1][len(ctoks[1])-4:]==self.acct_tail:
					# remove double quotes
					sctoks=[c.strip().replace('"','') for c in ctoks]
					table.append(sctoks[:len(fido_column_map)])

		return(table)
