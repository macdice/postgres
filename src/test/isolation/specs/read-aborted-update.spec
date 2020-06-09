# Test for a bug detected by Jepsen/Elle:
#
# https://www.postgresql.org/message-id/db7b729d-0226-d162-a126-8a8ab2dc4443%40jepsen.io

setup
{
	CREATE TABLE x (id text PRIMARY KEY, value text);
	CREATE TABLE y (id text PRIMARY KEY, value text);
	CREATE TABLE z (id text PRIMARY KEY, value text);
    INSERT INTO x VALUES ('x', 'initial value');
}

teardown
{
	DROP TABLE tab;
}

# Transaction 0 reads x and then writes y.
session "t0"
setup		{ BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step "t0rx" { SELECT * FROM tab WHERE id = 'x'; }
step "t0wy" { INSERT INTO tab2 VALUES ('y', 'foo'); }
step "t0c"  { COMMIT; }

# Transaction 1 reads and then writes x.
session "t1"
setup 		{ BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step "t1ry"	{ SELECT * FROM tab2 WHERE id = 'y'; }
step "t1rx"	{ SELECT * FROM tab WHERE id = 'x'; }
step "t1wx"	{ UPDATE tab SET value = 's1 was here' WHERE id = 'x'; }
step "t1c" 	{ COMMIT; }

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
step "t3ry"	{ SELECT * FROM tab WHERE id = 'y'; }
step "t3c"	{ COMMIT; }

# t0 < t1
# t1 < t3
# t3 < t0 (!) -- lost edge because t2 hides t0's write, due to the bug
# t2 abort

permutation "t0rx" "t0wy" "t1ry" "t1rx" "t1wx" "t1c" "t2wx" "t2a" "t3rx" "t3wx" "t0c" "t3c"
