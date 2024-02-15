import beancount.reports.report as rp
import beancount as bc
from beancount.core.data import Posting,Open,Transaction,Balance,Commodity,Price,Event
from beancount.parser import printer
from beancount.ingest.similar import find_similar_entries
from beancount.core.data import D, Decimal
from beancount.core import amount
import numpy as np 
import re
import os,sys
import yaml
from datetime import datetime as dt
from datetime import timedelta
from collections import OrderedDict

dir_path = "" # set this after import
numeric_regex="[0-9]+"
remove_duplicates=True
remove_zero_value_transactions=True
missing_payee_tag="UNASSIGNED"
quiet=True

def is_check(e):
	""" Determines if it is a check from narration and returns check number
		or None if not a check
	"""
	cn = None
	if type(e)==Transaction:
		checkno_match = re.search('CHECK\s+('+numeric_regex+')',e.narration.upper())
		if checkno_match:
			cn=int(checkno_match.groups()[0])
	return(cn)
	
def assign_check_payees(extracted_entries,account,filename=""):
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
		cn = is_check(e)
		if cn:
			if cn in payees_for_check:
				# format payee / memo / Check #
				nsplt=e.narration.split('/')
				memo_str=""
				if len(nsplt) > 1:
					memo_str=nsplt[1]
				nstr=" / ".join([payees_for_check[cn],memo_str,' '.join(["Check",str(cn)])])
				new_entry=e._replace(narration=nstr)
			else: # unassigned check number
				# previously categorized, probably by Quicken
				if "category" in e.meta:
					unassigned_checks[cn]=e.meta['category']
				else:
					amt=""
					if len(e.postings)>0:
						amt = e.postings[0].units
					unassigned_checks[cn]=missing_payee_tag + " # " + e.date.isoformat()+","+str(amt)
		# FIXME: special case for some banks - no check number
		elif type(e)==Transaction and re.match("CHECK$",e.narration.upper()):
			new_entry=e._replace(narration="COUNTER CASH")
		new_entries.append(new_entry)
	# track unassigned checks
	if len(unassigned_checks) > 0:
		hints=len([x for x in unassigned_checks if missing_payee_tag in unassigned_checks[x]])
		sys.stderr.write("Found {0} unassigned checks, {1} without hints for {2} ({3},{4} entries) for file {5}\n".format(len(unassigned_checks),str(hints),account,len(extracted_entries),len(new_entries),filename))
		with open(os.path.join(dir_path,check_file_prefix+"_unassigned.yaml"),"a") as f:
			f.write("# Unassigned check payees for {0}\n".format(filename))
			# order by check number, not item order
			od=OrderedDict(sorted(unassigned_checks.items()))
			for k,v in od.items():
				f.write(str(k)+":"+(6-len(str(k)))*" "+v+"\n")
 
	return(new_entries)


default_account_open_date='2000-01-01'

# problematic regex or yaml characters - replace with '.'
ry_chars=['#','*',',',"'",':','[',']','{','}','^','$','?','+','&','-','(',')']
# TODO: make configurable
annoying_prefixes=['Checkcard[ ]+[0-9]+ ','CHECKCARD[ ]+[0-9]+ ','Select Purchase. ','Debit Card Purchase. ','Sou ','ElectCHK [0-9]+ ','Cns ']

def regexify(s):
	""" Replaces regex special characters that conflict with yaml
		or do unwanted regex operations
		Args: s = string, part of narration that will become regex search
		Returns: cleaned up regex pattern that should work as yaml
		Notes: Will replace all numbers with generic [0-9]{n} pattern 
	"""
	rets=s
	if rets:
		rets=s.strip()
		if len(rets)==0:
			return("EMPTY")
		for c in ry_chars:
			rets=rets.replace(c,".")
		for p in annoying_prefixes:
			ps=re.match(p, rets)
			if ps:
				rets=rets[ps.span()[1]:] # remove the prefix
				break
		# replace specific numbers with generic regex match
		# If it is just a bunch of numbers, don't replace with generic
		if not re.search("[A-Za-z]+",rets):
			# explicit match, e.g. "A  / ..."
			if len(rets) < 4 and len(rets) > 0: 
				rets='^'+rets+'$' # '[ ]+/'
			return(rets)
		sp=0
		if not "CHECK " in rets.upper(): # don't replace check numbers
			# TODO: make better
			while True:
				sr=re.search('[0-9]+', rets[sp:])
				if sr:
					span=sr.span()
					sl=rets[span[0]+sp:span[1]+sp]
					nnum=span[1]-span[0]
					tail_rets=rets[sp:].replace(sl,'[0-9]{'+str(nnum)+'}',1)
					rets=rets[:sp]+tail_rets
					repnum=7+len(str(nnum))
					sp=sp+span[1]+(repnum-nnum) # rets could grow or shrink
					if sp >= len(rets)-1:
						break
				else:
					break
	# final clean up
	if len(rets) < 4 and len(rets) > 0: # make it an explicit match 
		rets="^"+rets+'$' # "[ ]+/"
	return(rets)

def group_regex(yaml_dict):
	""" Organizes regex patterns from simplest to most complex

		Arguments: yaml_dict, loaded from file where keys are regex patterns

		Returns: dict where each value is a list of regex patterns that
				match the key
	
		Notes:
			Example: re.search("Foo","FooBar") is not None
			If the search field has "FooBar", we want to match that,
			not "Foo" (which also matches)
			To disambiguate, take longest match after checking all patterns 
			in list
			
			Note that yaml_util should be used to remove rules that assign
			to the same account (e.g. if "Foo" and "FooBar" both map to
			"Expenses:FooX" then only the "Foo" rule is needed)
	"""
	return_dict={}
	for p1 in yaml_dict:
		return_dict[p1]=[]
		for p2 in yaml_dict:
			if p1!=p2 and re.search(p1,p2):
				return_dict[p1].append(p2)
	return(return_dict)

def best_match(e,pattern,alt_list):
	""" Finds best match for this pattern given a list of possible all_entries
		Args: entry, string pattern, and list of alt patterns

		Returns: best_match which is key to assign dict
				An empty string if no match is found
	"""
	best_pattern=""
	sr=None
	# try 'payee' part of payee / memo / Check #
	id_str = e.narration.split(' / ')[0].strip() 
	sr=re.search(pattern, id_str)
	if not sr and 'category' in e.meta:
		sr=re.search(pattern,e.meta['category'])
	if sr:
		best_pattern=pattern
		alt_sr=[]
		for alt_p in alt_list:
			alt_sr.append(re.search(alt_p,id_str))
		spans=[]
		for r in alt_sr:
			if r:
				spans.append(r.span()[1]-r.span()[0])
			else:
				spans.append(0)
		max_span=sr.span()[1]-sr.span()[0]
		for p,span in zip(alt_list,spans):
			if span > max_span:
				best_pattern=p
				max_span=span
	
	return(best_pattern)

def update_posting(e,account):
	if len(e.postings)!=2:
		pval=sum([p.units[0] for p in e.postings])
		pu=e.postings[0].units.currency
	else:
		pval= -e.postings[1].units[0]
		pu = e.postings[1].units.currency
	units=amount.Amount(-pval,pu)
	new_posting = Posting(account,units,None,None,None,{})
	if len(e.postings)!=2: # add additional posting
		e.postings.append(new_posting)
	else: # overwrite second posting
		e.postings[1]=new_posting
	return

def assign_entry(e, assignLUT, assign_groups):
	""" Tries to assign an entry to an account using patterns
		Arguments: 
			e (entry), 
			assignLUT (dict of patterns:account),
			assign_groups: grouping of patterns from least to most complex
		Returns: True if assigned, False otherwise
	"""
	# FIXME: Compile and turn pattern into one long reference
	assigned = False
	for pattern in assignLUT:
		best_pattern = best_match(e, pattern, assign_groups[pattern])
		if len(best_pattern) > 0:
			update_posting(e, assignLUT[best_pattern])
			assigned = True
			break
	return(assigned)

def update_unassigned(e, unassigned_payees):
	""" Updates the unassigned table
	"""
	pre_assigned_category="Expenses:UNASSIGNED"
	unassigned_payee="EMPTY"
	if 'category' in e.meta:
		# if it is a valid top-level account, use as-is
		if e.meta['category'].split(':')[0] in ['Assets','Expenses','Liabilities','Income','Equity']:
			pre_assigned_category=e.meta['category']
		else: # else assume it is an Expense
			pre_assigned_category=':'.join(["Expenses",e.meta['category']])
		unassigned_payee=e.meta['category']
	# first is always payee
	tok=e.narration.split('/')[0].strip() 
	if len(tok)!=0: # use 1st field of narration 
		unassigned_payee=tok
	reg_key = regexify(unassigned_payee)
	if reg_key in unassigned_payees and pre_assigned_category!=unassigned_payees[reg_key]:
		sys.stderr.write("Warning: Ambiguous regex key {0}: was {1} now {2}\n".format(reg_key,unassigned_payees[reg_key],pre_assigned_category))
		# the original reg key is ambiguous, so create key from cat
		# otherwise the ambiguous key would be the first assignment in file
		# which works but probably isn't wanted - best to delete the ambiguous
		# key if possible
		if not unassigned_payees[reg_key] in unassigned_payees:
			unassigned_payees[unassigned_payees[reg_key]]=unassigned_payees[reg_key]
		reg_key=pre_assigned_category # use this as new key in unassigned
	unassigned_payees[reg_key]=pre_assigned_category
	return

def save_unassigned(unassigned_payees,account_file,ex_file):
	""" Saves unassigned payees to appended yaml file
	"""
	oup=OrderedDict(sorted(unassigned_payees.items()))
	if len(oup) > 0:
		with open(os.path.splitext(account_file)[0]+"_unassigned.yaml","a") as f:
			f.write("# Unassigned accounts for {0}\n".format(ex_file)) 
			for k in oup:
				if len(k) < 40:
					f.write("\""+k+"\"" + ":" + (40-len(k))*" " + oup[k] + "\n")
				else: # really long key...
					f.write("\""+k+"\"" + ": " + oup[k] + "\n")
	return

def assign_accounts(extracted_entries_list,ledger_entries,filename_accounts):
	""" Assigns accounts from payee field and open any new accounts
	"""
# now assign possible missing postings
# grab accounts from all postings so far
	new_entries=[] # list of (file, entries[]) tuples
	opened_accounts=[]
	if ledger_entries:
		opened_accounts=[e.account for e in ledger_entries if type(e)==Open]
	for (ex_file,entries),(fn,account) in zip(extracted_entries_list,filename_accounts):
		# links payees/narration to account 
		assignLUT={}
		assign_groups={}
		unassigned_payees={}
		# default account file name for unassigned
		account_file=os.path.join(dir_path,account.replace(':','_')+".yaml")
		if os.path.isfile(account_file):
			with open(account_file,'r') as f:
				assignLUT=yaml.safe_load(f)
				assign_groups=group_regex(assignLUT)
		new_entries.append((ex_file,[]))
		for en,e in enumerate(entries):
			# check for zero value entries - lots of these in CC's
			if type(e)==Transaction and len(e.postings)==1 and e.postings[0].units[0]==0 and remove_zero_value_transactions: 
				continue
			if type(e)==Transaction and unbalanced(e.postings):
				if not assign_entry(e,assignLUT,assign_groups):
					update_unassigned(e,unassigned_payees)
			if type(e)==Open:
				if not e.account in opened_accounts:
					opened_accounts.append(e.account)

		if len(unassigned_payees) > 0:
			sys.stderr.write("Found {0} unassigned accounts for {1} ({2} entries) for file {3}\n".format(len(unassigned_payees),account,len(entries),ex_file))
			save_unassigned(unassigned_payees,account_file,ex_file)

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

def unbalanced(postings):
	""" Returns true of transaction is unbalanced
	"""
	ret=False
	# Simple case: only 1 posting then by defn unbalanced
	if len(postings)==1:
		ret=True
	# see if sum of transactions are balanced in right currency
	else:
		delta= sum([p.units[0] for p in postings if p.units and p.units[0]])
		matched = set()
		[matched.add(p.units[1]) for p in postings if p.units and p.units[0]]
		if delta!=D(0) and len(matched)==1:
			ret=True
	return(ret)

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
					acct="Empty"
					units="Empty"
					if ne.postings and len(ne.postings) > 0:
						acct = ne.postings[0].account
						units=str(ne.postings[0].units)
					ident= acct + " " + units + " " + ne.narration
				elif type(ne)==Open:
					ident="Open: " + ne.account
				elif type(ne)==Balance:
					ident="Balance: " + ne.account
				elif type(ne)==Commodity:
					ident="Commodity: " + ne.currency
				elif type(ne)==Price:
					ident="Price: " + ne.currency
				else:
					ident=str(type(ne))
				if not remove_duplicates:
					msg="Marked dup {0} {1}\n".format(ne.date,ident)
					sys.stderr.write(msg)
					deduped_entries_list[-1][1].append(ne)
				else:
					msg="Removed dup {0} {1}\n".format(ne.date,ident)
					if not quiet:
						sys.stderr.write(msg)
#					ne=Note({'orig':ne.narration},ne.date,ne.postings[0].account,'Dup is {0} {1} {2}'.format(str(d[0].date),d[0].postings[0].account,d[0].narration))
#					deduped_entries_list[-1][1].append(ne)
			else:
				deduped_entries_list[-1][1].append(ne)

    # have to tack on last set if multiple files ingested with no ledger
	if ledger_entries==None and len(extracted_entries_list)>1:
		deduped_entries_list.append(extracted_entries_list[-1])
		
	return(deduped_entries_list)

compare_delta = Decimal("0.0100001")

def compare_entries(entries_a,entries_b):
	""" Compares two lists of entries to see if there are duplicates 
		Arguments: 
			entries_a,entries_b: lists of entries to compare
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
			# FIXME: This can fail for two e.g. Cash transactions of the
			# same amount on the same day (e.g. buying $10 of X and $10 of Y)
			# Need to check all postings of transaction
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
					if vn and va and vn.currency==va.currency and abs(vn.number-va.number) < compare_delta and ne_ts==da: 
						dl.append(ne)
		elif type(ea)==Balance or type(ea)==Open: 
			for ne in near_entries:
				if type(ne)==type(ea):
					if ea.account==ne.account and ea.date==ne.date:
						dl.append(ne)
		elif type(ea)==Commodity or type(ea)==Price:
			for ne in near_entries:
				if type(ne)==type(ea):
					if ea.currency==ne.currency and ea.date==ne.date:
						dl.append(ne)
		elif type(ea)==Event:
			for ne in near_entries:
				if type(ne)==type(ea):
					if ea.type==ne.type and ea.date==ne.date and ea.description==ne.description:
						dl.append(ne)

	return(duplicate_list)
