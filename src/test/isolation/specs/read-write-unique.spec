# Read-write-unique test.
#
# This test has two serializable transactions: T1 observes that a key
# value doesn't exist in a table, then T2 inserts the value and commits,
# then T1 tries to insert the value.
#
# ...

setup
{
  CREATE TABLE test (i int PRIMARY KEY);
}

teardown
{
  DROP TABLE test;
}

session "s1"
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "r1" { SELECT * FROM test WHERE i = 42; }
step "w1" { INSERT INTO test VALUES (42); }
step "c1" { COMMIT; }

session "s2"
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "w2" { INSERT INTO test VALUES (42); }
step "c2" { COMMIT; }

permutation "r1" "w2" "c2" "w1" "c1"

