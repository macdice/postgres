#ifndef CAR_H
#define CAR_H

#include "lib/ilist.h"

struct car_control;
typedef struct car_control car_control;

/* A type that can represent the index to objects in the cache, and -1. */
typedef int car_index;

/*
 * The user of this module should have a mapping table, and embed this object
 * into its mapping table entries.  They must live at a stable address.  The
 * contents are private should not be accessed directly.
 *
 * XXX Other designs are possible, such as using an index into an array of
 * these; that'd support use in open-addressing hash tables and avoid the need
 * for dlist_node (could use something smaller based on ints) but would
 * require an extra memory indirection.
 */
typedef struct car_mapping
{
	dlist_node	node;
	car_index	index;
	uint8		list;
	bool		reference;
} car_mapping;

static inline car_index
car_mapping_index(const car_mapping *mapping)
{
	return mapping->index;
}

/*
 * After looking up an object in a mapping table twice the size of the cache
 * and finding an entry, the caller must call this function to find the
 * object's index in the cache (for example, buffer number), or learn that
 * it's not currently in the cache.
 *
 * Return -1 if the object is not currently cached, and an object index if it
 * is.  The reference flag is set to affect future replacement decisions.
 */
static inline car_index
car_access(car_mapping *mapping)
{
	mapping->reference = true;

	return mapping->index;
}

typedef enum car_allocate_result
{
	CAR_ALLOCATE_OK,
	CAR_ALLOCATE_REPLACE,
	CAR_ALLOCATE_FORGET,
	CAR_ALLOCATE_FAIL
} car_allocate_result;

extern size_t car_estimate_size(int objects);
extern void car_init(car_control *ctl, int objects);
extern int	car_mappings(const car_control *ctl);
extern void car_mapping_init(car_mapping *mapping);
extern void car_forget(car_control *ctl, car_mapping *mapping);
extern car_allocate_result car_allocate(car_control *ctl, car_mapping *mapping, car_mapping **replace);
extern void car_complete_replace(car_control *ctl, car_mapping *mapping, bool accepted);

#endif
