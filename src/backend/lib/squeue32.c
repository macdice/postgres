/*
 * An implementation of the algorithm from the paper "A practical nonblocking
 * queue algorithm using compare-and-swap" by Shann, Huang and Chen.  It
 * provides a fixed-sized queue of uint32 values, for multiple producers and
 * multiple consumers.
 *
 * The paper has {val, ref} items that can be atomically compared-and-swapped,
 * where val is a pointer to user data with NULL to indicate an empty slot, and
 * ref is a counter to avoid the ABA problem.  Instead, we encode the item into
 * a single uint64, but we steal the most significant bit from ref to indicate
 * an occupied slot.  That way we reserve the full range of a uint32 for user
 * data.
 */

#include "postgres.h"

#include "lib/squeue32.h"
#include "port/atomics.h"

struct squeue32 {
	size_t		size;
	pg_atomic_uint64 front;
	pg_atomic_uint64 rear;
	pg_atomic_uint64 data[FLEXIBLE_ARRAY_MEMBER];
};

static inline bool
squeue32_item_is_empty(uint64 item)
{
	/* The slot is empty if it doesn't have the occupied bit set. */
	return (item & 0x80000000) == 0;
}

static inline uint32
squeue32_item_value(uint64 item)
{
	return item >> 32;
}

static inline uint64
squeue32_clear_item(uint64 old_item)
{
	uint32 ref = old_item & 0x7fffffff;

	return ref + 1;
}

static inline uint64
squeue32_update_item(uint32 value, uint64 old_item)
{
	uint32 ref = old_item & 0x7fffffff;

	return ((uint64) value << 32) | 0x80000000 | (ref + 1);
}

/*
 * How much memory does it take to hold a queue with the given number of items?
 */
size_t
squeue32_estimate(size_t size)
{
	return offsetof(squeue32, data) + sizeof(pg_atomic_uint64) * size;
}

/*
 * Initialize a queue in a memory region that is sufficent in size according to
 * squeue32_estimate().
 */
void
squeue32_init(squeue32 *queue, size_t size)
{
	queue->size = size;
	pg_atomic_init_u64(&queue->front, 0);
	pg_atomic_init_u64(&queue->rear, 0);
	for (size_t i = 0; i < size; ++i)
		pg_atomic_init_u64(&queue->data[i], 0);
}

/*
 * Enqueue a value.  Returns true on success, and false if the queue is full.
 */
bool
squeue32_enqueue(squeue32 *queue, uint32 value)
{
	uint64		rear;
	uint64		x;

enq_try_again:
	rear = pg_atomic_read_u64(&queue->rear);
	x = pg_atomic_read_u64(&queue->data[rear % queue->size]);
	pg_read_barrier();		/* XXX: not mentioned in paper, but maybe needed on non-TSO? */
	if (rear != pg_atomic_read_u64(&queue->rear))
		goto enq_try_again;
	if (rear == (pg_atomic_read_u64(&queue->front) + queue->size))
		return false;
	if (squeue32_item_is_empty(x))
	{
		/* Try to store an item. */
		if (pg_atomic_compare_exchange_u64(&queue->data[rear % queue->size],
										   &x,
										   squeue32_update_item(value, x)))
		{
			/* Try to increment REAR. */
			pg_atomic_compare_exchange_u64(&queue->rear, &rear, rear + 1);
			return true;
		}
	}
	else
	{
		/* Help others increment REAR. */
		pg_atomic_compare_exchange_u64(&queue->rear, &rear, rear + 1);
	}
	goto enq_try_again;
}

/*
 * Dequeue a value.  Returns true on success, and false if the queue is empty.
 */
bool
squeue32_dequeue(squeue32 *queue, uint32 *value)
{
	uint64		front;
	uint64		x;

deq_try_again:
	front = pg_atomic_read_u64(&queue->front);
	x = pg_atomic_read_u64(&queue->data[front % queue->size]);
	pg_read_barrier();		/* XXX: not mentioned in paper, but maybe needed on non-TSO? */
	if (front != pg_atomic_read_u64(&queue->front))
		goto deq_try_again;
	if (front == pg_atomic_read_u64(&queue->rear))
		return false;
	/* Is the front of the list non-empty? */
	if (!squeue32_item_is_empty(x))
	{
		/* Try to remove an item. */
		if (pg_atomic_compare_exchange_u64(&queue->data[front % queue->size],
										   &x,
										   squeue32_clear_item(x)))
		{
			/* Try to increment FRONT. */
			pg_atomic_compare_exchange_u64(&queue->front, &front, front + 1);
			*value = squeue32_item_value(x);
			return true;
		}
		else
		{
			/* Help others increment FRONT. */
			pg_atomic_compare_exchange_u64(&queue->front, &front, front + 1);
		}
	}
	goto deq_try_again;
}
