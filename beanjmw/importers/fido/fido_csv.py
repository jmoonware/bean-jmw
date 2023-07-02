# custom importer to load Fildelity CSV brokerage account history

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType
from beancount.core.number import MISSING

import os,sys

from datetime import datetime as dt

# remove these chars as Beancount accounts can't have them
quicken_category_remove=[' ','\'','&','-','+','.']

# all possible actions for investments
investment_actions={
'BUY':'Buy', 
'YOU BOUGHT':'Buy', 
'YOU SOLD':'Sell',
'LONG-TERM CAP GAIN':'CGLong',
'SHORT-TERM CAP GAIN':'CGShort',
'DIVIDEND RECEIVED':'Div',
'INTEREST':'IntInc',
'REINVESTMENT':'Reinv',
'ELECTRONIC FUNDS TRANSFER':'Xout',
'DIRECT DEBIT':'Xout',
}

default_open_date='2000-01-01'

fido_cols = ['Run Date', 'Account', 'Action', 'Symbol', 'Security Description', 'Security Type', 'Quantity', 'Price ($)', 'Commission ($)', 'Fees ($)', 'Accrued Interest ($)', 'Amount ($)', 'Settlement Date']

fido_row_fields = ['date', 'account', 'action', 'symbol', 'security_description', 'type', 'quantity', 'price', 'commission', 'fees', 'accrued_interest', 'amount', 'settlement_date']

from collections import namedtuple
FidoRow = namedtuple('FidoRow',fido_row_fields)

class Importer(ImporterProtocol):
	def __init__(self,account_name,currency='USD'):
		self.account_name=account_name
		self.acct_tok=self.account_name.split(':')[-1]
		self.acct_tail=self.acct_tok[len(self.acct_tok)-4:] 
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
			while ln < len(head_lines):
				if 'Brokerage' in head_lines[ln]:
					break
				ln+=1
			for l in head_lines[ln+3:]:
				toks=l.split(',')
				if len(toks) > 1:
					fa=toks[1]
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
		entries=[]
		try:
			with open(file.name,'r') as f:
				lines=f.readlines()
		except:
			sys.stderr.write("Unable to open or parse {0}".format(file.name))
			return(entries)
		import_table=self.create_table(lines)
#		securities=self.get_securities() # might need for opens
		entries = self.get_transactions(import_table)

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
		for fr in map(FidoRow._make,table): 
			meta={"lineno":0,"filename":self.account_name}
			narration_str=" / ".join([fr.account,fr.action])
			tn=Transaction(
				meta=meta,
				date=dt.date(dt.strptime(fr.date,'%m/%d/%Y')),
				flag="*",
				payee="Investment",
				narration=narration_str,
				tags=EMPTY_SET,
				links=EMPTY_SET,
				postings=self.generate_investment_postings(fr),
			)
			entries.append(tn)

		return(entries)

	def generate_investment_postings(self,fr):
		postings=[]

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
		sec_name=fr.security_description
		symbol = fr.symbol
		sec_currency=symbol
		sec_account=symbol
		if "CASH" in fr.security_description: # special case
			sec_account="Cash"
			sec_currency=self.currency
		acct = ":".join([self.account_name, sec_account])
		# open account with this currency
		self.account_currency[acct]=sec_currency
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
			amt=Decimal(0)
			if len(fr.amount)>0:
				amt=Decimal(fr.amount)
			prc=Decimal(0)
			if len(fr.price)>0:
				prc=Decimal(fr.price)
			acct = ":".join([self.account_name, sec_account])
			postings[0]=p0._replace(
				account = acct,
				units=Amount(Decimal(fr.quantity),sec_currency),
				price = Amount(prc,self.currency),
			)
			aname='Cash'
			# shares in came from a share exchange somewhere else
			if fido_action == 'ShrsIn': # KLUDGE
				aname = 'Transfer'
			postings[1]=p1._replace(
				account = ":".join([self.account_name,aname]),
				units = Amount(-amt,self.currency)
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
				units=Amount(-Decimal(fr.quantity),sec_currency),
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
		elif fido_action =='Reinv': # ReinvDiv,ReinvLg,ReinvMd,ReinvSh... 
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Income'),sec_account,fido_action]),
				units=Amount(-Decimal(fr.amount),self.currency)
			)
			postings[1]=p1._replace(
				account = ":".join([self.account_name, sec_account]),
				units=Amount(Decimal(fr.quantity),sec_currency),
				cost=Cost(Decimal(fr.price),self.currency,dt.date(dt.strptime(fr.date,'%m/%d/%Y')),""),
			)
		elif fido_action=='ReinvInt': 
			postings[0]=p0._replace(
				account = ":".join([self.account_name,"IntInc"]),
				units=Amount(-Decimal(fr.amount),self.currency)
			)
			postings[1]=p1._replace(
				account = ":".join([self.account_name, "Cash"]),
				units=Amount(Decimal(fr.quantity),sec_currency),
			)
		elif fido_action in ['Xin','Xout']: 
			postings[0]=p0._replace(
				account = ":".join([self.account_name,"Cash"]),
				units=Amount(Decimal(fr.amount),self.currency)
			)
			postings[1]=p1._replace(
				account = ":".join([self.account_name, "Transfer"]),
				units=Amount(-Decimal(fr.quantity),sec_currency),
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


	def create_table(self,lines):
		""" Returns a list of (mostly) unparsed string tokens
	        each item in the table is a list of tokens exactly 
			len(fido_row_fields) long
			Arguments:
				lines: list of raw lines from csv file
		"""
		table=[]
		nl=0
		while nl < len(lines):
			if "Brokerage" in lines[nl]:
				break
			nl+=1

		# make sure the columns haven't changed... 
		is_fido=True
		cols=[c.strip() for c in lines[nl+2].split(',')]
		for c,fc in zip(cols,fido_cols):
			if c!=fc:
				is_fido=False
				break
		if not is_fido or len(cols)!=len(fido_cols):
			sys.stderr.write("Bad format {0}".format(cols))
			return(table)
	
		# it's got the right columns, now extract the data	
		for l in lines[nl+3:]:
			ctoks=l.split(',')
			if len(ctoks) >= len(fido_cols):
				if ctoks[1][len(ctoks[1])-4:]==self.acct_tail:
					# remove double quotes
					sctoks=[c.strip().replace('"','') for c in ctoks]
					table.append(sctoks[:len(fido_row_fields)])

		return(table)
