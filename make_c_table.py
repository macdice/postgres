import sys

def write_table():
	ranges = []
	order = 0
	begin_order = -2
	begin_codepoint = -2
	end_codepoint = -2
	for line2 in sys.stdin.readlines():
		line2 = line2[:-1]
		if len(line2) > 0:
			order += 1
			codepoint = ord(line2)
			if codepoint == end_codepoint + 1:
				# extend range
				end_codepoint = codepoint
			else:
				# dump previous range or item
				if end_codepoint >= 0:
					ranges.append((begin_codepoint, end_codepoint, begin_order))
				# start new range
				begin_order = order
				begin_codepoint = codepoint
				end_codepoint = codepoint
	# dump final range
	ranges.append((begin_codepoint, end_codepoint, begin_order))
	ranges.sort()
	for begin, end, order in ranges:
		print("	{{0x%08x, 0x%08x}, %d}," % (begin, end, order))

if __name__ == "__main__":
	write_table()
