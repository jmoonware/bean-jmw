# sort/combine/simplify util for yaml account assignment files 

import yaml
import argparse
import sys
import re
from collections import OrderedDict

ap=argparse.ArgumentParser()

ap.add_argument("--existing","-e",required=True,help='Existing yaml file for account',default='')
ap.add_argument("--add","-a",required=False,help='More entries to add, usually from unassigned file',default='')
ap.add_argument("--override","-o",required=False,help='Use these entries to override any unassigned in existing',default='')
ap.add_argument("--column_width","-cw",required=False,help='Align second column to this value',default=0)
ap.add_argument("--overwrite","-ow",required=False,help='Quietly overwrite entries that are added, otherwise error if in existing',default=False,action='store_true')
ap.add_argument("--similar","-sp",required=False,help='Check regex patterns for subsetting and remove redundant rules',default=False,action='store_true')

ap.add_argument("--invert","-iv",required=False,help='Organize by account, not regex',default=False,action='store_true')

clargs=ap.parse_args(sys.argv[1:])

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

	sys.stderr.write("Found {0} comments\n".format(len(retc)))
	return(retc)

with open(clargs.existing,'r') as f:
	ex_entries=yaml.safe_load(f)
	comments_dict=load_comments(f)

add_entries={}
if len(clargs.add)>0:
	with open(clargs.add,'r') as f:
		add_entries=yaml.safe_load(f)
	for ae in add_entries:
		if ae in ex_entries and not clargs.overwrite:
			msg="Duplicate entry {0} for existing {1} (trying to add {2})\n".format(ae,ex_entries[ae],add_entries[ae])
			raise ValueError(msg)
		ex_entries[ae]=add_entries[ae] 

override_entries={}
if len(clargs.override)>0:
	with open(clargs.override,'r') as f:
		override_entries=yaml.safe_load(f)

# override logic
for oe in override_entries:
	if oe in ex_entries:
		if "UNASSIGNED" in ex_entries[oe]:
			ex_entries[oe]=override_entries[oe] 

sorted_entries=OrderedDict(sorted(ex_entries.items()))

n_int=sum([True for k in sorted_entries if type(k)==int])
if n_int/len(sorted_entries) > 0.99: # all integers
	col_width=6
	quotes=""
else:
	col_width=40
	quotes="\""

# override value if set on command line
if clargs.column_width!=0:
	col_width=int(clargs.col_width)

# find out if any regexes are a subset of one another...
# if the simpler regex pattern assigns to the same account
# using a more complex pattern, then remove the more complex pattern
simplified_entries=OrderedDict(sorted_entries)
if clargs.similar:
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

if clargs.invert:
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
		for vi in v:
			print(" - "+"\""+vi+"\"")

else:
	# output combined, sorted, possibly simplified items, preserving comments
	for k,v in simplified_entries.items():
		nspace=max(col_width-len(str(k)),1)
		comment=""
		if k in comments_dict and v=="UNASSIGNED":
			comment=' # '+comments_dict[k]
		s_out = quotes + str(k) + quotes + ":" + " "*nspace + v + comment
		print(s_out)

