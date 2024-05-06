# sort/combine/simplify util for yaml account assignment files 

import yaml
import argparse
import sys
import re
from collections import OrderedDict
import numpy as np

ap=argparse.ArgumentParser()

ap.add_argument("--existing","-e",required=True,help='Existing yaml file for account',default='')
ap.add_argument("--remap","-r",required=False,help='yaml file that remaps one account to another (each line is existing: remapped)',default='')
ap.add_argument("--add","-a",required=False,help='More entries to add, usually from unassigned file, can be comma delimited list',default='')
ap.add_argument("--override","-o",required=False,help='Use these entries to override any unassigned in existing',default='')
ap.add_argument("--column_width","-cw",required=False,help='Align second column to this value',default=0)
ap.add_argument("--overwrite","-ow",required=False,help='Quietly overwrite entries that are added, otherwise error if in existing',default=False,action='store_true')
ap.add_argument("--similar","-sp",required=False,help='Check regex patterns for subsetting and remove redundant rules',default=False,action='store_true')
ap.add_argument("--invert","-iv",required=False,help='Organize by account, not regex',default=False,action='store_true')
ap.add_argument("--chartofaccounts","-coa",required=False,help='Just list accounts - useful for remapping',default=False,action='store_true')

clargs=ap.parse_args(sys.argv[1:])

replace_chars=["'"]

# loads a dict of possible in-line comments in the existing file
# these contain date, amount info for unassigned transactions
def load_comments(f):
	retc={}
	f.seek(0)
	lines=f.readlines()
	for l in lines:
		sl=l.strip()
		if len(sl)>0 and sl[0]=='#':
			continue
		toks=sl.split(': ')
		if len(toks)>1:
			pc=toks[1].split('#') # this line has a comment
			if len(pc) > 1:
				if re.match("[0-9]+$",toks[0]): # a numerical value 
					k=int(toks[0])
				else: # quoted regex string 
					k=toks[0][1:-1] # remove quotes
				retc[k]=pc[1]

	sys.stderr.write("{0}: Found {1} comments\n".format(clargs.existing,len(retc)))
	return(retc)

with open(clargs.existing,'r') as f:
	ex_entries=yaml.safe_load(f)
	comments_dict=load_comments(f)

add_entries={}
if len(clargs.add)>0:
	add_files=clargs.add.split(',')
	for add_file in add_files:
		with open(add_file,'r') as f:
			add_entries=yaml.safe_load(f)
		for ae in add_entries:
			if ae in ex_entries:
				if ex_entries[ae]!=add_entries[ae]:
					msg="Duplicate entry {0} for existing {1} (trying to add {2})\n".format(ae,ex_entries[ae],add_entries[ae])
					if not clargs.overwrite:
						raise ValueError(msg)
			ex_entries[ae]=add_entries[ae] 

override_entries={}
if len(clargs.override)>0:
	with open(clargs.override,'r') as f:
		override_entries=yaml.safe_load(f)

# override logic
# will override UNASSIGNED entries with new account or payee
# if the key (which is a regex string or check number) exactly matches
for oe in override_entries:
	if oe in ex_entries:
		if "UNASSIGNED" in ex_entries[oe]:
			ex_entries[oe]=override_entries[oe] 

if ex_entries == None or len(ex_entries)==0:
	sys.stderr.write("{0}: No entries\n".format(clargs.existing))
	sys.exit()
	
sorted_entries=OrderedDict(sorted(ex_entries.items()))

# check if it is a check number or regex string yaml file
n_int=sum([True for k in sorted_entries if type(k)==int])
if n_int/len(sorted_entries) > 0.99: # all integers
	col_width=6
	quotes=""
	is_checkno=True
else:
	col_width=40
	quotes="\'"
	is_checkno=False

# override value if set on command line
if clargs.column_width!=0:
	col_width=int(clargs.col_width)

# find out if any regexes are a subset of one another...
# if the simpler regex pattern assigns to the same account
# using a more complex pattern, then remove the more complex pattern
simplified_entries=OrderedDict(sorted_entries)
if clargs.similar and not is_checkno:
	remove_pattern={}
	similar_patterns={}
	removed=0
	for p1 in sorted_entries:
		similar_patterns[p1]=[]
		for p2 in sorted_entries:
			if p1!=p2 and re.search(p1,p2):
				similar_patterns[p1].append(p2)
				# mark entries that are assigning to exact same account
				if sorted_entries[p1]==sorted_entries[p2]:	
					remove_pattern[p2]=True
					removed += 1
	simplified_entries=OrderedDict()
	for e in sorted_entries:
		if not e in remove_pattern:
			simplified_entries[e]=sorted_entries[e]
	print("# yaml_util: Removed {0} of {1} redundant regex patterns".format(removed,len(sorted_entries)))

# this remaps the existing entries account values if a remap file is present
if len(clargs.remap) > 0:
	with open(clargs.remap) as f:
		remap_dict=yaml.safe_load(f)
	acct_vals=np.array(simplified_entries.values())
	for ra in remap_dict:
		if remap_dict[ra] and ra in acct_vals:
			acct_vals[acct_vals==ra]=remap_dict[ra]
	for ia,ea in enumerate(simplified_entries):
		simplified_entries[ea]=acct_vals[ia]

if clargs.invert or clargs.chartofaccounts:
	# print by account
	by_account={}
	for k,v in simplified_entries.items():
		if v in by_account:
			by_account[v].append(k)
		else:
			by_account[v]=[k]

	sorted_by_account=OrderedDict(sorted(by_account.items()))
	for k,v in sorted_by_account.items():
		print(k+":")
		if clargs.invert:
			for vi in v:
				print(" - " + quotes + vi + quotes)

else:
	# output combined, sorted, possibly simplified items, preserving comments
	for k,v in simplified_entries.items():
		nspace=max(col_width-len(str(k)),1)
		comment=""
		if k in comments_dict and v=="UNASSIGNED":
			comment=' # '+comments_dict[k]
		fk = k
		fv = ""
		if v != None:
			fv = v
		if type(k) == str:
			for rc in replace_chars:
				fk=fk.replace(rc,".")
		s_out = quotes + str(fk) + quotes + ":" + " "*nspace + fv + comment
		print(s_out)

