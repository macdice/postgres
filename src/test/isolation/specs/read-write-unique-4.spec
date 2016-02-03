# Read-write-unique test.
# Implementing a gapless sequence of ID numbers for each year.

setup
{
  CREATE TABLE invoice (
    year int,
    invoice_number int,
    PRIMARY KEY (year, invoice_number)
  );

  INSERT INTO invoice VALUES (2016, 1), (2016, 2);
}

teardown
{
  DROP TABLE invoice;
}

session "s1"
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "r1" { SELECT COALESCE(MAX(invoice_number) + 1, 1) FROM invoice WHERE year = 2016; }
step "w1" { INSERT INTO invoice VALUES (2016, 3); }
step "c1" { COMMIT; }

session "s2"
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "r2" { SELECT COALESCE(MAX(invoice_number) + 1, 1) FROM invoice WHERE year = 2016; }
step "w2" { INSERT INTO invoice VALUES (2016, 3); }
step "c2" { COMMIT; }

# if they both read first then there should be an SSI conflict
permutation "r1" "r2" "w1" "w2" "c1" "c2"

# if s2 doesn't both to read first, then inserting 3 should generate a unique constraint failure
permutation "r1" "w1" "w2" "c1" "c2"

# if s1 doesn't both to read first, but s2 does, then s1 got lucky and s2 should still experience an SSI failure
# TODO: but in fact it still experiences a unique constraint violation...
permutation "r2" "w1" "w2" "c1" "c2"
