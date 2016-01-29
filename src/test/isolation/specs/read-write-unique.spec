# Read-write-unique test.
#
# Two SSI transactions see that there is no row with value 42
# in the table, then try to insert that value; T1 inserts,
# and then T2 blocks waiting for T1 to commit.  Finally,
# T2 reports a unique constraint violation.
#
# Ideally what T2 would do is detect a conflict out and report
# a serialization failure.

setup
{
  CREATE TABLE test (i integer PRIMARY KEY);
}

teardown
{
  DROP TABLE test;
}

session "s1"
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "r1" { SELECT * FROM test; }
step "w1" { INSERT INTO test VALUES (42); }
step "c1" { COMMIT; }

session "s2"
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "r2" { SELECT * FROM test; }
step "w2" { INSERT INTO test VALUES (42); }
step "c2" { COMMIT; }

permutation "r1" "r2" "w1" "w2" "c1" "c2"
