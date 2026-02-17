SELECT getdatabaseencoding() <> 'UTF8' AS skip_test \gset
\if :skip_test
\quit
\endif

SELECT U&'\0061\0308bc' <> U&'\00E4bc' COLLATE "C" AS sanity_check;

SELECT unicode_version() IS NOT NULL;
SELECT unicode_assigned(U&'abc');
SELECT unicode_assigned(U&'abc\+10FFFF');

SELECT normalize('');
SELECT normalize(U&'\0061\0308\24D1c') = U&'\00E4\24D1c' COLLATE "C" AS test_default;
SELECT normalize(U&'\0061\0308\24D1c', NFC) = U&'\00E4\24D1c' COLLATE "C" AS test_nfc;
SELECT normalize(U&'\00E4bc', NFC) = U&'\00E4bc' COLLATE "C" AS test_nfc_idem;
SELECT normalize(U&'\00E4\24D1c', NFD) = U&'\0061\0308\24D1c' COLLATE "C" AS test_nfd;
SELECT normalize(U&'\0061\0308\24D1c', NFKC) = U&'\00E4bc' COLLATE "C" AS test_nfkc;
SELECT normalize(U&'\00E4\24D1c', NFKD) = U&'\0061\0308bc' COLLATE "C" AS test_nfkd;

SELECT "normalize"('abc', 'def');  -- run-time error

SELECT U&'\00E4\24D1c' IS NORMALIZED AS test_default;
SELECT U&'\00E4\24D1c' IS NFC NORMALIZED AS test_nfc;

SELECT num, val,
    val IS NFC NORMALIZED AS NFC,
    val IS NFD NORMALIZED AS NFD,
    val IS NFKC NORMALIZED AS NFKC,
    val IS NFKD NORMALIZED AS NFKD
FROM
  (VALUES (1, U&'\00E4bc'),
          (2, U&'\0061\0308bc'),
          (3, U&'\00E4\24D1c'),
          (4, U&'\0061\0308\24D1c'),
          (5, '')) vals (num, val)
ORDER BY num;

SELECT is_normalized('abc', 'def');  -- run-time error

-- Interesting thresholds for UTF-8 and UTF-16 encoding

WITH octet_length_thresholds(t, description) AS (VALUES
  (U&'\+000001', 'First 1-byte UTF-8 sequence supported by PostgreSQL'),
  (U&'\+00007F', 'Final 1-byte UTF-8 sequence'),
  (U&'\+000080', 'First 2-byte UTF-8 sequence'),
  (U&'\+0007FF', 'Final 2-byte UTF-8 sequence'),
  (U&'\+000800', 'First 3-byte UTF-8 sequence'),
  (U&'\+00FFFF', 'Final 3-byte UTF-8 sequence (end of BMP)'),
  (U&'\+010000', 'First 4-byte UTF-8 sequence, UTF-16 pair'),
  (U&'\+10FFFF', 'Final valid codepoint'))
SELECT to_hex(ascii(t)),
       description,
       octet_length(t::text) AS utf8_octets,
       octet_length(t::utf16) AS utf16_octets,
       length(t::text) AS utf8_length,
       length(t::utf16) AS utf16_length
FROM octet_length_thresholds;
-- Out of range codepoints
SELECT U&'\+000000';
SELECT U&'\+110000';

CREATE FUNCTION check_text_op(left_string text,
                              left_type text,
                              right_string text,
                              right_type text,
                              op text)
RETURNS boolean
LANGUAGE plpgsql
AS
$$
DECLARE
  format text;
  text_e text;
  text_i int;
  text_b boolean;
  test_e text;
  test_i int;
  test_b boolean;
BEGIN
  -- all cross-type results against text, text
  IF op = 'cmp' THEN
    format := '%s%scmp(''%s''::%s, ''%s''::%s)';
    text_e := format(format, 'bttext', '', left_string, 'text', right_string, 'text');
    EXECUTE format('SELECT sign(%s)', text_e) INTO text_i;
    test_e := format(format, left_type, CASE WHEN left_type = right_type THEN '' ELSE right_type END, left_string, left_type, right_string, right_type);
    EXECUTE format('SELECT sign(%s)', test_e) INTO test_i;
    IF test_i <> text_i THEN
      RAISE NOTICE '% -> %, but % -> %', text_e, text_i, test_e, test_i;
    END IF;
  ELSE
    format := '''%s''::%s %s ''%s''::%s';
    text_e := format(format, left_string, 'text', op, right_string, 'text');
    EXECUTE format('SELECT %s', text_e) INTO text_b;
    test_e := format(format, left_string, left_type, op, right_string, right_type);
    EXECUTE format('SELECT %s', test_e) INTO test_b;
    IF test_b <> text_b THEN
      RAISE NOTICE '% -> %, but % -> %', text_e, text_b, test_e, test_b;
    END IF;
  END IF;
  RETURN true;
END;
$$;

WITH strings (s) AS (VALUES ('a'), ('aa'), ('aaa')),
     ops (o)     AS (VALUES ('<'), ('<='), ('='), ('<>'), ('>='), ('>'), ('cmp')),
     types (t)   AS (VALUES ('text'), ('name'), ('utf16'))
SELECT count(check_text_op(left_string.s,
                           left_type.t,
                           right_string.s,
                           right_type.t,
                           op.o))
FROM       strings left_string
CROSS JOIN strings right_string
CROSS JOIN types   left_type
CROSS JOIN types   right_type
CROSS JOIN ops     op
WHERE left_type.t = 'utf16' OR right_type.t = 'utf16';

WITH examples(language, string) AS
(VALUES
  ('English',  'In a hole in the ground there lived a hobbit.'),
  ('Spanish',  'En un agujero en el suelo, vivía un hobbit.'),
  ('Russian',  'В норе под землей жил-был хоббит.'),
  ('Arabic',   'كان يعيش هوبيت في حفرة في الأرض.'),
  ('Hebrew',   'בתוך חור באדמה חי הוביט.'),
  ('Greek',    'Σε μια τρύπα στο έδαφος ζούσε ένα χόμπιτ.'),
  ('Korean',   '땅속 어느 구멍에 한 호빗이 살고 있었다.'),
  ('Hindi',    'जमीन में बने एक गड्ढे में एक हॉबिट रहता था।'),
  ('Tamil',    'அந்த நிலத்தில் ஒரு துளையில் ஒரு ஹாபிட் வசித்து வந்தது.'),
  ('Chinese',  '在地下一个洞里，住着一个霍比特人。'),
  ('Japanese', '穴のなかに、ひとりのホビットが暮らしていた。'))
SELECT language,
       octet_length(string::text) || '→' || octet_length(string::utf16) AS octets,
       to_char(ROUND(100 *
                     ((octet_length(string::utf16)::float /
                      (octet_length(string::text)::float) - 1.0))),
               'S999%') AS delta,
       string
FROM examples;
