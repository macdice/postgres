# Test for a bug detected by Jepsen/Elle:
#
# https://www.postgresql.org/message-id/db7b729d-0226-d162-a126-8a8ab2dc4443%40jepsen.io
#

setup
{
	CREATE TABLE tab (id text PRIMARY KEY, value text);
    INSERT INTO tab VALUES ('x', 'initial value');
}

teardown
{
	DROP TABLE tab;
}

# Transaction 1 reads and then writes x.
session "t1"
setup 		{ BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step "t1rx"	{ SELECT * FROM tab WHERE id = 'x'; }
step "t1wx"	{ UPDATE tab SET value = 's1 was here' WHERE id = 'x'; }
step "t1c" 	{ COMMIT; }

#step "s1wx"	{ UPDATE t1 SET value = '..' WHERE id = 'x'; }

# Transaction 2 writes x, but then aborts.
session "t2"
setup		{ BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step "t2wx"	{ UPDATE tab SET value = 's2 was here' WHERE id = 'x'; }
step "t2a"	{ ABORT; }

# Transaction 3 reads and then writes x.
session "t3"
setup		{ BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step "t3rx"	{ SELECT * FROM tab WHERE id = 'x'; }
step "t3wx"	{ UPDATE tab SET value = 's3 was here' WHERE id = 'x'; }
step "t3c"	{ COMMIT; }

permutation "t1rx" "t3rx" "t1wx" "t1c" "t2wx" "t2a" "t3rx" "t3wx" "t3c"
