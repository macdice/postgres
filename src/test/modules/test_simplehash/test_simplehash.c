#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "common/hashfn.h"

PG_MODULE_MAGIC;

#define DEFINE_ELEMENT_TYPE(prefix, key_type, element_size)		\
	typedef struct prefix##element_size##_entry					\
	{															\
		key_type key;											\
		char data[(element_size) - sizeof(int)];				\
	} prefix##element_size##_entry

#define DEFINE_TEST(prefix, element_size, make_key, free_key, key_var, set_key) \
	static void															\
	prefix##element_size(int nelements)									\
	{																	\
		prefix##element_size##_hash *hash;								\
		hash = prefix##element_size##_create(CurrentMemoryContext,		\
											 nelements,					\
											 NULL);						\
		for (int i = 0; i < nelements; ++i)								\
		{																\
			bool found;													\
			prefix##element_size##_entry *entry;						\
			entry = prefix##element_size##_insert(hash,					\
												  make_key(i),			\
												  &found);				\
			entry->data[0] = 0;											\
		}																\
																		\
		elog(NOTICE, #prefix #element_size " with nelements = %d",		\
			 nelements);												\
		/* XXX TODO probe loop with timing */							\
																		\
		prefix##element_size##_destroy(hash);							\
	}

#define MAKE_KEY_OID(i) i
#define FREE_KEY_OID(i) i
#define KEY_VAR_OID() Oid key
#define SET_KEY_OID(i) key = i
#define DEFINE_TEST_OID(prefix, element_size)							\
	DEFINE_TEST(prefix, \
				element_size, \
				MAKE_KEY_OID, \
				FREE_KEY_OID, \
				KEY_VAR_OID, \
				SET_KEY_OID)

/* Define struct test_oidN_entry for various N. */
DEFINE_ELEMENT_TYPE(test_oid, Oid, 8);
DEFINE_ELEMENT_TYPE(test_oid, Oid, 16);
DEFINE_ELEMENT_TYPE(test_oid, Oid, 24);
DEFINE_ELEMENT_TYPE(test_oid, Oid, 32);
DEFINE_ELEMENT_TYPE(test_oid, Oid, 40);

#define SH_PREFIX test_oid8
#define SH_ELEMENT_TYPE test_oid8_entry
#define SH_KEY key
#define SH_KEY_TYPE Oid
#define SH_KEY_EMPTY_VALUE InvalidOid
#define SH_HASH_KEY(t, key) murmurhash32(key)
#define SH_EQUAL(t, a, b) ((a) == (b))
#define SH_DECLARE
#define SH_DEFINE
#define SH_SCOPE static inline
#include "lib/simplehash.h"

#define SH_PREFIX test_oid16
#define SH_ELEMENT_TYPE test_oid16_entry
#define SH_KEY key
#define SH_KEY_TYPE Oid
#define SH_KEY_EMPTY_VALUE InvalidOid
#define SH_HASH_KEY(t, key) murmurhash32(key)
#define SH_EQUAL(t, a, b) ((a) == (b))
#define SH_DECLARE
#define SH_DEFINE
#define SH_SCOPE static inline
#include "lib/simplehash.h"

#define SH_PREFIX test_oid24
#define SH_ELEMENT_TYPE test_oid24_entry
#define SH_KEY key
#define SH_KEY_TYPE Oid
#define SH_KEY_EMPTY_VALUE InvalidOid
#define SH_HASH_KEY(t, key) murmurhash32(key)
#define SH_EQUAL(t, a, b) ((a) == (b))
#define SH_DECLARE
#define SH_DEFINE
#define SH_SCOPE static inline
#include "lib/simplehash.h"

#define SH_PREFIX test_oid32
#define SH_ELEMENT_TYPE test_oid32_entry
#define SH_KEY key
#define SH_KEY_TYPE Oid
#define SH_KEY_EMPTY_VALUE InvalidOid
#define SH_HASH_KEY(t, key) murmurhash32(key)
#define SH_EQUAL(t, a, b) ((a) == (b))
#define SH_DECLARE
#define SH_DEFINE
#define SH_SCOPE static inline
#include "lib/simplehash.h"

#define SH_PREFIX test_oid40
#define SH_ELEMENT_TYPE test_oid40_entry
#define SH_KEY key
#define SH_KEY_TYPE Oid
#define SH_KEY_EMPTY_VALUE InvalidOid
#define SH_HASH_KEY(t, key) murmurhash32(key)
#define SH_EQUAL(t, a, b) ((a) == (b))
#define SH_DECLARE
#define SH_DEFINE
#define SH_SCOPE static inline
#include "lib/simplehash.h"

/* Define test_oidN for various N. */
DEFINE_TEST_OID(test_oid, 8);
DEFINE_TEST_OID(test_oid, 16);
DEFINE_TEST_OID(test_oid, 24);
DEFINE_TEST_OID(test_oid, 32);
DEFINE_TEST_OID(test_oid, 40);

PG_FUNCTION_INFO_V1(test_simplehash);

Datum
test_simplehash(PG_FUNCTION_ARGS)
{
	for (int n = 16; n <= (1 << 16); n *= 2)
	{
		test_oid8(n);
		test_oid16(n);
		test_oid24(n);
		test_oid32(n);
		test_oid40(n);
	}

	PG_RETURN_NULL();
}
