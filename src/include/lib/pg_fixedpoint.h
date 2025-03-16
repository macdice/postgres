#ifndef PG_FIXEDPOINT_H
#define PG_FIXEDPOINT_H

/* Fixed-point arithmetic type, half of it used for a binary fraction. */
typedef int32 pg_fixed32;

#define PG_FIXED32_SCALE (UINT32_C(1) << 16)

static inline pg_fixed32
pg_fixed32_from_int(int32 i)
{
	return (pg_fixed32) i * PG_FIXED32_SCALE;
}

static inline pg_fixed32
pg_fixed32_from_double(double d)
{
	return d * PG_FIXED32_SCALE;
}

static inline pg_fixed32
pg_fixed32_mul(pg_fixed32 a, pg_fixed32 b)
{
	return (pg_fixed32) (((int64) a * (int64) b) / PG_FIXED32_SCALE);
}

static inline pg_fixed32
pg_fixed32_square(pg_fixed32 fp)
{
	return pg_fixed32_mul(fp, fp);
}

static inline pg_fixed32
pg_fixed32_ceil(pg_fixed32 fp)
{
	return (fp + PG_FIXED32_SCALE - 1) & ~(PG_FIXED32_SCALE - 1);
}

static inline int32
pg_fixed32_floor_int(pg_fixed32 fp)
{
	return fp / PG_FIXED32_SCALE;
}

static inline int32
pg_fixed32_round_int(pg_fixed32 fp)
{
	return pg_fixed32_floor_int(fp + PG_FIXED32_SCALE / 2);
}

static inline double
pg_fixed32_to_double(pg_fixed32 fp)
{
	return (double) fp / PG_FIXED32_SCALE;
}

#endif
