import beancount.reports.report as rp
import beancount as bc
from beancount.core.data import Posting,Open,Transaction,Balance
from beancount.parser import printer
from beancount.ingest.similar import find_similar_entries
from beancount.core.data import D
from beancount.core import amount
import numpy as np 
import re
import os,sys
import yaml
from datetime import datetime as dt
from datetime import timedelta
from collections import OrderedDict

dir_path = "" # set this after import
numeric_regex="^[0-9]+$"
remove_duplicates=True
missing_payee_tag="UNASSIGNED"

def assign_check_payees(extracted_entries,account):
	""" Assign check payees from a yaml file by number
		Call this function only for checking accounts
		Arguments:
			extracted_entries_list: list of entries for checking account
			account: string name of account
		Returns:
			new list of entries
		Notes:
			This will create an <Account_Name>_payees_unassigned.yaml file
			for check numbers that are not assigned a payee
			If the check has metadata (from say a Quicken export) then this
			is used to create a "hint" in the payee column of the unassigned 
			yaml file.
	"""
	payees_for_check={}
	check_file_prefix=account.replace(":","_")+"_payees"
	check_file=os.path.join(dir_path,check_file_prefix+".yaml")
	if os.path.isfile(check_file):
		with open(check_file) as f:
			payees_for_check=yaml.safe_load(f)
	else:
		sys.stderr.write("Warning: Can't find check number to payee file {0}\n".format(check_file))
	
	# assign payees on checks
	new_entries=[] # list of entries
	unassigned_checks={}
	for e in extracted_entries:
		new_entry=e
		if type(e)==Transaction:
			if "CHECK" in e.narration.upper():
				toks=e.narration.split()
				if len(toks) > 1:
					checkno_match=re.match(numeric_regex,toks[1])
					if checkno_match:
						cn=int(checkno_match.string)
						if cn in payees_for_check:
							new_entry=e._replace(narration="CHECK {0} / {1}".format(checkno_match.string,payees_for_check[cn]))
						else: # unassigned check number
							# previously categorized, probably by Quicken
							if "category" in e.meta:
								unassigned_checks[cn]=e.meta['category']
							else:
								unassigned_checks[cn]=missing_payee_tag + " # " + dt.date(e.date).isoformat()
				elif re.match("^CHECK$",e.narration.upper()):
					new_entry=e._replace(narration="CHECK / CASH")
		new_entries.append(new_entry)
	# track unassigned checks
	if len(unassigned_checks) > 0:
		hints=len([x for x in unassigned_checks if missing_payee_tag in unassigned_checks[x]])
		sys.stderr.write("Found {0} unassigned checks, {1} without hints for {2} ({3},{4} entries)\n".format(len(unassigned_checks),str(hints),account,len(extracted_entries),len(new_entries)))
		with open(os.path.join(dir_path,check_file_prefix+"_unassigned.yaml"),"w") as f:
			f.write("# Unassigned check payees\n")
			# order by check number, not item order
			od=OrderedDict(sorted(unassigned_checks.items()))
			for k,v in od.items():
				f.write(str(k)+":"+(6-len(str(k)))*" "+v+"\n")
 
	return(new_entries)


default_account_open_date='2000-01-01'

def assign_accounts(extracted_entries_list,ledger_entries,filename_accounts):
	""" Assigns accounts from payee field and open any new accounts
	"""
# now assign possible missing postings
# grab accounts from all postings so far
	new_entries=[] # list of (file, entries[]) tuples
	opened_accounts=[]
	if ledger_entries:
		opened_accounts=[e.account for e in ledger_entries if type(e)==Open]
	for ex_file, entries in extracted_entries_list:
		# links payees/narration to account 
		assignLUT={}
		unassigned_payees={}
		# default account file name for unassigned
		account='unknown'
		account_file=os.path.join(dir_path,account+".yaml")
		if ex_file in filename_accounts:
			account=filename_accounts[ex_file]
			account_file=os.path.join(dir_path,account.replace(':','_')+".yaml")
			if os.path.isfile(account_file):
				with open(account_file,'r') as f:
					assignLUT=yaml.safe_load(f)
		new_entries.append((ex_file,[]))
		for e in entries:
			if type(e)==Transaction and (sum([p.units[0] for p in e.postings])!=D(0) or len(e.postings)==1):
				assigned=False	
				# this is where we check for the regex patterns
				for pattern in assignLUT:
					if re.search(pattern, e.narration):
						pval=sum([p.units[0] for p in e.postings])
						units=amount.Amount(-pval,"USD")
						new_posting = Posting(assignLUT[pattern],units,None,None,None,{})
						e.postings.append(new_posting)
						assigned=True
						break
				if not assigned:
					pre_assigned_category="Expenses:"
					if 'category' in e.meta:
						pre_assigned_category+=(e.meta['category'].replace(" ",""))
					if "CHECK" in e.narration.upper(): 
						toks=e.narration.split('/')
						if len(toks) > 1:
							unassigned_payees[toks[1].strip()]=pre_assigned_category
						else:
							unassigned_payees[toks[0].strip()]=pre_assigned_category
					else:
						unassigned_payees[e.narration.split('/')[0].strip()]=pre_assigned_category
			if type(e)==Open:
				if not e.account in opened_accounts:
					opened_accounts.append(e.account)
		# unassigned payees
		oup=OrderedDict(sorted(unassigned_payees.items()))
		if len(oup) > 0:
			sys.stderr.write("Found {0} unassigned accounts for {1} ({2} entries)\n".format(len(oup),account,len(entries)))
			with open(os.path.splitext(account_file)[0]+"_unassigned.yaml",'w') as f:
				f.write("# Unassigned accounts\n") 
				for k in oup:
					f.write(k + ":" + (40-len(k))*" " + oup[k] + "\n")
		new_entries[-1][1].extend(entries)

	# see what accounts we have
	account_list=[]
	for ex_file, entries in extracted_entries_list:
		for e in entries:
			if type(e)==Transaction:
				for p in e.postings:
					if not p.account in account_list:
						account_list.append(p.account)
	# open accounts that aren't already open
	open_entries=[Open({},default_account_open_date,a,["USD"],None) for a in account_list if not a in opened_accounts]

	return([("new_opens",open_entries)]+new_entries)

def deduplicate(extracted_entries_list,ledger_entries):
	""" Removes or marks entries if they exist in another ingest or ledger
		Arguments:
			extracted_entries_list: [(fn,entries[])] list from ingest
			ledger_entries: list of entries from ledger
		Returns:
			Possibly modified extracted_entries_list
	"""
	# all_entries is a list of entry lists, one list for each file
	all_entries_list=[x[1] for x in extracted_entries_list]
	# if we have a ledger, add this list as the last one
	if ledger_entries:
		all_entries_list.append(ledger_entries)
	# need at least two entry lists to compare...
	if len(all_entries_list)<2:
		return([[[] for _ in range(len(extracted_entries_list[0][1]))]])
	#
	# Compare the list of ingested to each other, and possibly an existing 
	# ledger
	# There are two different kinds of duplicates -
    # (1) Reimported (from say two ingestion events of same account)
    # (2) Transactions counted in two different accounts
	# 
	# Type 2 duplicates are dealt with via a third account
	# e.g. for paying a credit card from checking, both should be assigned
	# to a third account like "Expenses:Card:Payment which sums to zero
	# 
	# We also don't need to compare records within an extracted account
    # as duplicates should not occurr
	#
	duplicates=[]
	for i,a in enumerate(all_entries_list[:-1]):
		a_groups=[]
		for b in all_entries_list[i+1:]:
			a_groups.append(compare_entries(a, b))
		
		# entries_a might have been compared to multiple other entry lists
		dl_a=[[] for _ in range(len(a))]
		for i,dl in enumerate(dl_a): 
			for ag in a_groups:
				dl.extend(ag[i])
		duplicates.append(dl_a)		

    # Do something about duplicates
	deduped_entries_list=[]

	for fn_entries,dups in zip(extracted_entries_list,duplicates):
		deduped_entries_list.append((fn_entries[0],[]))
		for e,d in zip(fn_entries[1],dups):
			ne=e
			if len(d) > 0: # have at least one duplicate
				ne=e._replace(meta={"mark":"Duplicate"})
#				sys.stderr.write("Found dup: {0} {1}\n".format(ne,d))
				if type(ne)==Transaction:
					ident="Transaction: " + ne.narration
				elif type(ne)==Open:
					ident="Open: " + ne.account
				elif type(ne)==Balance:
					ident="Balance: " + ne.account
				else:
					ident=str(type(ne))
				if not remove_duplicates:
					msg="Marked dup {0} {1}\n".format(ne.date,ident)
					sys.stderr.write(msg)
					deduped_entries_list[-1][1].append(ne)
				else:
					msg="Removed dup {0} {1}\n".format(ne.date,ident)
					sys.stderr.write(msg)
#					ne=Note({'orig':ne.narration},ne.date,ne.postings[0].account,'Dup is {0} {1} {2}'.format(str(d[0].date),d[0].postings[0].account,d[0].narration))
#					deduped_entries_list[-1][1].append(ne)
			else:
				deduped_entries_list[-1][1].append(ne)

    # have to tack on last set if multiple files ingested with no ledger
	if ledger_entries==None and len(extracted_entries_list)>1:
		deduped_entries_list.append(extracted_entries_list[-1])
		
	return(deduped_entries_list)

def compare_entries(entries_a,entries_b):
	""" Compares two lists of entries to see if there are duplicates 
		Arguments: 
			entries_b,entries_b: lists of entries to compare
		Returns:
			lists of duplicates, one list for each entry in entries_a
		Notes:
			For transactions between say, checking autopay and credit cards,
			create a 3rd account (e.g. Expenses:Card:Payment) that should sum
			to zero - otherwise removing one of them will cause problems on 
			re-ingesting
	"""
	# this is what gets returned
	duplicate_list=[[] for _ in range(len(entries_a))]
	dateval_a=[]
	dateval_b=[]
	# TODO: Use pytz and convert to UTC 
	# beancount doesnt use full times, only dates
	for e in entries_a:
		dateval_a.append(dt.timestamp(dt.fromisoformat(str(e.date))))	
	for e in entries_b:
		dateval_b.append(dt.timestamp(dt.fromisoformat(str(e.date))))	
	td1=3600*24 # 1 day in seconds
	dateval_a=np.array(dateval_a)
	dateval_b=np.array(dateval_b)
	for ea,da,dl in zip(entries_a,dateval_a,duplicate_list):
		f=(dateval_b>=(da-td1))&(dateval_b<=(da+td1))
		near_entries=[entries_b[i] for i,fi in enumerate(f) if fi]
		if type(ea)==Transaction:
			aa=None
			va=None
			if len(ea.postings) > 0:
				aa=ea.postings[0].account
				va=ea.postings[0].units 
			for ne in near_entries:
				if type(ne)==Transaction:
					ne_ts=dt.timestamp(dt.fromisoformat(str(ne.date)))
					vn=None
					for p in ne.postings:
						if p.account==aa:
							vn=p.units
					# date, amount, and accounts match
					if vn and vn==va and ne_ts==da: 
						dl.append(ne)
		elif type(ea)==Balance or type(ea)==Open:
			for ne in near_entries:
				if type(ne)==type(ea):
					if ea.account==ne.account and ea.date==ne.date:
						dl.append(ne)

	return(duplicate_list)
