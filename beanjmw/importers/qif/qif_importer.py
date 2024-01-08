# custom importer to load QIF files exported from Quicken
# use the custom qifparse package in https://github.com/jmoonware/qifparse

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType
from beancount.core.number import MISSING

import os,sys

from qifparse.parser import QifParser
from qifparse.qif import Transaction as QifTransaction
from qifparse.qif import Investment as QifInvestment

from datetime import datetime as dt

# remove these chars as Beancount accounts can't have them
quicken_category_remove=[' ','\'','&','-','+','.']

# all possible actions for qif investments
qif_investment_actions=[
'Buy', 'BuyX', 'Sell',  'SellX', 'CGLong', 'CGLongX', 'CGMid', 'CGMidX', 'CGShort', 'CGShortX', 'Div', 'DivX', 'IntInc', 'IntIncX', 'ReinvDiv', 'ReinvInt', 'ReinvLg', 'ReinvMd', 'ReinvSh', 'Reprice', 'XIn', 'XOut', 'MiscExp', 'MiscExpX', 'MiscInc', 'MiscIncX', 'MargInt', 'MargIntX', 'RtrnCap', 'RtrnCapX', 'StkSplit', 'ShrsOut', 'ShrsIn', 'Cash',
]

default_open_date='2000-01-01'

class Importer(ImporterProtocol):
	def __init__(self,account_name,currency='USD'):
		self.account_name=account_name
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
		if not qif:
			sys.stderr.write("Unable to open or parse {0}".format(file.name))
			return(entries)
		securities=qif.get_securities() # may be in qif export
		for tno, qt in enumerate(qif.get_transactions(True)[0]):
			memo_str=""
			if qt.memo:
				memo_str=qt.memo.strip().replace('/','.')
			meta=new_metadata(file.name, tno)
			if type(qt)==QifTransaction:
				num_str=""
				if qt.num:
					num_str=qt.num.strip()
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
				if len(num_str) > 0:
					num_str=" "+num_str
				payee_str=""
				check_str=""
				if qt.payee:
					payee_str=qt.payee.strip().replace('/','.')
					if "CHECK" in qt.payee.upper():
						check_str="Check"
				n_toks=[payee_str,memo_str,check_str+num_str]
				 # truly blank
				if len(''.join(n_toks))==0 and not 'category' in meta:
					n_toks[0]='EMPTY' # for assigning later
				narration_str=" / ".join(n_toks)
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
			elif type(qt)==QifInvestment:
				act_str=""
				if qt.action:
					act_str=qt.action.strip()
				n_toks=[memo_str,act_str]
				 # truly blank
				if len(''.join(n_toks))==0 and not 'category' in meta:
					n_toks[0]='EMPTY' # for assigning later
				narration_str=" / ".join(n_toks)
				tn=Transaction(
					meta=meta,
					date=dt.date(qt.date),
					flag="*",
					payee="Investment from QIF",
					narration=narration_str,
					tags=EMPTY_SET,
					links=EMPTY_SET,
					postings=self.generate_investment_postings(qt, self.account_name, securities),
				)
				entries.append(tn)

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

	def generate_investment_postings(self,qt,account_name,security_list):
		postings=[]
		if not qt.action in qif_investment_actions:
			sys.stderr.write("Unknown inv action: {0} in {1}\n".format(qt.action,qt))
			return(postings)
	
		# set defaults for two generic postings (p0, p1)
		sec_name="UNKNOWN"
		if qt.security:
			sec_name=qt.security
		symbol = [s.symbol for s in security_list if s.name and s.name == qt.security]
		sec_currency="UNKNOWN"
		sec_account="UNKNOWN"
		if len(symbol) > 0 and symbol[0]:
			sec_currency=symbol[0] # should only be one
			sec_account=symbol[0] # same as currency, except for Cash
			acct = ":".join([account_name, sec_account])
			# open account with this currency
			self.account_currency[acct]=sec_currency
		if qt.action == "Cash":
			sec_account = "Cash"
			sec_currency = self.currency
		qty = Decimal('0')
		if qt.quantity:
			qty = qt.quantity
		map_reinv = {
			'ReinvDiv':'Div',
			'ReinvLg':'CGLong',
			'ReinvMd':'CGMid',
			'ReinvSh':'CGShort',
		} 
		postings.append(
			Posting(
				account = account_name,
				units=Amount(qty,sec_currency),
				cost=None,
				price=None,
				flag=None,
				meta={}
			)
		)
		postings.append(
			Posting(
				account = account_name + ":Cash",
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
		if qt.action in ['Buy','ShrsIn']:
			amt=Decimal(0)
			if qt.amount:
				amt=qt.amount
			prc=Decimal(0)
			if qt.price:
				prc=qt.price
			nprc=prc
			acct = ":".join([account_name, sec_account])
			meta = new_metadata(acct,0)
			if qt.quantity > 0:
				nprc = abs(amt/qt.quantity)
				if abs(qt.quantity*(nprc-prc)) > 0.0025: # FIXME
					meta['rounding']="Price was {0}".format(prc)
			postings[0]=p0._replace(
				account = acct,
				units=Amount(qt.quantity,sec_currency),
#				cost=Cost(qt.price,self.currency,dt.date(qt.date),""),
				price = Amount(nprc,self.currency),
				meta = meta,
			)
			aname='Cash'
			# shares in came from a share exchange somewhere else
			if qt.action == 'ShrsIn': # KLUDGE
				if qt.memo and "TRANSFER" in qt.memo.upper():
					aname = 'Transfer'
			postings[1]=p1._replace(
				account = ":".join([account_name,aname]),
				units = Amount(-amt,self.currency)
			)
		elif qt.action=='Sell': 
			commission=Decimal(0)
			if qt.commission:
				commission=qt.commission
				postings.append(
					Posting(
						account = account_name.replace('Assets','Expenses') + ":Commission",
						units=Amount(commission,self.currency),
						cost=None,
						price=None,
						flag=None,
						meta={}
					)
				)
			total_cost=commission
			if qt.amount:
				total_cost=qt.amount+commission
			prc=Decimal(0)
			if qt.price:
				prc=qt.price
			postings[0]=p0._replace(
				account = ":".join([account_name, sec_account]),
				units=Amount(-qt.quantity,sec_currency),
		#		cost=None, # let Beancount FIFO booking rule take care
				price = Amount(prc,self.currency),
			)
			postings[1]=p1._replace(
				account = account_name + ":Cash",
				units = Amount(total_cost-commission,self.currency)
			)
			postings.append(
				Posting(
					account = account_name.replace('Assets','Income')+":PnL",
					units = NoneType(),
					cost = None,
					price = None,
					flag = None,
					meta=None,
				)
			)
		elif qt.action=='BuyX':
			pass
		elif qt.action=='SellX': 
			pass
		elif qt.action=='DivX': 
			pass
		elif qt.action=='IntIncX': 
			pass
		elif qt.action=='CGLongX': 
			pass
		elif qt.action=='CGMidX': 
			pass
		elif qt.action=='CGShortX': 
			pass
		elif qt.action in ['Div','CGShort','CGLong','CGMid']:
			postings[0]=p0._replace(
				account = ":".join([account_name.replace('Assets','Income'),sec_account,qt.action]),
				units=Amount(-qt.amount,self.currency)
			)
			postings[1]=p1._replace(
				account = account_name + ":Cash",
				units = Amount(qt.amount,self.currency)
			)
		elif qt.action in ['IntInc','MiscInc']:
			postings[0]=p0._replace(
				account = ":".join([account_name.replace('Assets','Income'),qt.action]),
				units=Amount(-qt.amount,self.currency)
			)
			postings[1]=p1._replace(
				account = account_name + ":Cash",
				units = Amount(qt.amount,self.currency)
			)
		elif qt.action in ['MiscExp']:
			postings[0]=p0._replace(
				account = ":".join([account_name.replace('Assets','Expenses'),qt.action]),
				units=Amount(-qt.amount,self.currency)
			)
			postings[1]=p1._replace(
				account = account_name + ":Cash",
				units = Amount(qt.amount,self.currency)
			)
		elif qt.action in ['ReinvDiv','ReinvLg','ReinvMd','ReinvSh']: 
			postings[0]=p0._replace(
				account = ":".join([account_name.replace('Assets','Income'),sec_account,map_reinv[qt.action]]),
				units=Amount(-qt.amount,self.currency)
			)
			meta = new_metadata(self.account_name, 0)
			nprc=qt.price
			if qt.quantity > 0:
				nprc = abs(qt.amount/qt.quantity)
				if abs(qt.quantity*(nprc-qt.price)) > 0.0025: # FIXME
					meta['rounding']="Price was {0}".format(qt.price)
			postings[1]=p1._replace(
				account = ":".join([account_name, sec_account]),
				units=Amount(qt.quantity,sec_currency),
#				cost=Cost(nprc,self.currency,dt.date(qt.date),""),
				price = Amount(nprc,self.currency),
				meta=meta,
			)
		elif qt.action=='ReinvInt': 
			postings[0]=p0._replace(
				account = ":".join([account_name,"IntInc"]),
				units=Amount(-qt.amount,self.currency)
			)
			postings[1]=p1._replace(
				account = ":".join(account_name, "Cash"),
				units=Amount(qt.quantity,sec_currency),
			)
		elif qt.action=='ReinvSh': 
			pass
		elif qt.action=='Reprice': 
			pass
		elif qt.action in ['XIn','XOut']: # Cash in or out
			amt = Decimal('0') # qt.amount may be empty!
			if qt.amount:
				amt = qt.amount
			if qt.action=='XOut':
				amt = -amt
			postings[0]=p0._replace(
				account = ":".join([account_name, "Cash"]),
				units=Amount(amt,self.currency),
			)
			postings[1]=p1._replace(
				account = ":".join([account_name,"Transfer"]),
				units = Amount(-amt,self.currency)
			)
		elif qt.action=='XOut': 
			pass
		elif qt.action=='MiscExpX': 
			pass
		elif qt.action=='MiscIncX': 
			pass
		elif qt.action=='MargInt': 
			pass
		elif qt.action=='MargIntX': 
			pass
		elif qt.action=='RtrnCap': 
			pass
		elif qt.action=='RtrnCapX': 
			pass
		elif qt.action=='StkSplit': 
			pass
		# just remove shares - manually fix where they go later!
		# looks like sale for transfer between e.g. share classes 
		elif qt.action=='ShrsOut': 
			amt = Decimal('0') # qt.amount may be empty!
			if qt.amount:
				amt = qt.amount
			price = Decimal('0')
			if qt.price:
				price = qt.price
			postings[0]=p0._replace(
				account = ":".join([account_name, sec_account]),
				units=Amount(-qt.quantity,sec_currency),
#				cost=Cost(price,self.currency,dt.date(qt.date),""),
				price = Amount(price,self.currency),
			)
			if qt.memo and "TRANSFER" in qt.memo.upper():
				acct="Transfer"
			else:
				acct="Cash"
			postings[1]=p1._replace(
				account = ":".join([account_name,acct]),
				units = Amount(amt,self.currency)
			)
		# not an official Qif action but still used...
		elif qt.action=='Cash':
			# might be goodwill, witholding, or some such
			# let regex assignment fill in later
			amt=Decimal(0)
			if qt.amount:
				amt=qt.amount
			postings[0]=p0._replace(
				account = account_name + ":Cash",
				units = Amount(amt,self.currency)
			)
			postings.remove(p1)
		else:
			sys.stderr.write("Unknown QIF investment action {0}\n".format(qt.action))
	
		return(postings)
