/*
 * Utilities for working with hash values.
 *
 * Portions Copyright (c) 2017-2021, PostgreSQL Global Development Group
 */

#ifndef HASHFN_H
#define HASHFN_H


/*
 * Rotate the high 32 bits and the low 32 bits separately.  The standard
 * hash function sometimes rotates the low 32 bits by one bit when
 * combining elements.  We want extended hash functions to be compatible with
 * that algorithm when the seed is 0, so we can't just do a normal rotation.
 * This works, though.
 */
#define ROTATE_HIGH_AND_LOW_32BITS(v) \
	((((v) << 1) & UINT64CONST(0xfffffffefffffffe)) | \
	(((v) >> 31) & UINT64CONST(0x100000001)))


extern uint32 hash_bytes(const unsigned char *k, int keylen);
extern uint64 hash_bytes_extended(const unsigned char *k,
								  int keylen, uint64 seed);
extern uint32 hash_bytes_uint32(uint32 k);
extern uint64 hash_bytes_uint32_extended(uint32 k, uint64 seed);

#ifndef FRONTEND
static inline Datum
hash_any(const unsigned char *k, int keylen)
{
	return UInt32GetDatum(hash_bytes(k, keylen));
}

static inline Datum
hash_any_extended(const unsigned char *k, int keylen, uint64 seed)
{
	return UInt64GetDatum(hash_bytes_extended(k, keylen, seed));
}

static inline Datum
hash_uint32(uint32 k)
{
	return UInt32GetDatum(hash_bytes_uint32(k));
}

static inline Datum
hash_uint32_extended(uint32 k, uint64 seed)
{
	return UInt64GetDatum(hash_bytes_uint32_extended(k, seed));
}
#endif

extern uint32 string_hash(const void *key, Size keysize);
extern uint32 tag_hash(const void *key, Size keysize);
extern uint32 uint32_hash(const void *key, Size keysize);

#define oid_hash uint32_hash	/* Remove me eventually */

/*
 * Combine two 32-bit hash values, resulting in another hash value, with
 * decent bit mixing.
 *
 * Similar to boost's hash_combine().
 */
static inline uint32
hash_combine(uint32 a, uint32 b)
{
	a ^= b + 0x9e3779b9 + (a << 6) + (a >> 2);
	return a;
}

/*
 * Combine two 64-bit hash values, resulting in another hash value, using the
 * same kind of technique as hash_combine().  Testing shows that this also
 * produces good bit mixing.
 */
static inline uint64
hash_combine64(uint64 a, uint64 b)
{
	/* 0x49a0f4dd15e5a8e3 is 64bit random data */
	a ^= b + UINT64CONST(0x49a0f4dd15e5a8e3) + (a << 54) + (a >> 7);
	return a;
}

/*
 * Simple inline murmur hash implementation hashing a 32 bit integer, for
 * performance.
 */
static inline uint32
murmurhash32(uint32 data)
{
	uint32		h = data;

	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;
	return h;
}

/*
 * A murmur hash implementation based on Austin Appleby's public domain
 * MurmurHash1.  Assumes that input is suitably aligned for uint32.  Results
 * not portable between endiannesses.
 */
static inline uint32
murmurhash_align4(const void *input, size_t len)
{
	uint32 h = 0 ^ (len * 0xc6a4a793);
	const unsigned char *data = (const unsigned char *) input;

	/*
	 * This function can't handle non-int-aligned input.  This variant of the
	 * function is intended to unroll to efficient code for structs that have
	 * suitable alignment, without runtime code to handle non-aligned leading
	 * bytes.
	 */
	Assert((((uintptr_t) input) % sizeof(uint32)) == 0);

	/* Handle uint32 words. */
	while (len >= 4)
	{
		uint32 k = *(uint32 *) data;

		h += k;
		h *= 0xc6a4a793;
		h ^= h >> 16;
		data += 4;
		len -= 4;
	}

	/* Handle leftover bytes at the end. */
	switch (len)
	{
	case 3:
		h += data[2] << 16;
		/* FALL THROUGH */
	case 2:
		h += data[1] << 8;
		/* FALL THROUGH */
	case 1:
		h += data[0];
		h *= 0xc6a4a793;
		h ^= h >> 16;
		/* FALL THROUGH */
	}

	h *= 0xc6a4a793;
	h ^= h >> 10;
	h *= 0xc6a4a793;
	h ^= h >> 17;

	return h;
}

#endif							/* HASHFN_H */
