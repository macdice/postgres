typedef struct GucTable
{
	bool		enable_seqsccan;
	bool		enable_indexscan;
	bool		enable_indexonlyscan;
	bool		enable_bitmapscan;
	bool		enable_tidscan;
	bool		enable_sort;
	bool		enable_incremental_sort;
	bool		enable_hashagg;
	bool		enable_material;
	bool		enable_resultcache;
	bool		enable_nestloop;
	bool		enable_mergejoin;
	bool		enable_hashjoin;
	bool		enable_gathermerge;
	bool		enable_partitionwise_join;
	bool		enable_partitionwise_aggregate;
	bool		enable_parallel_append;
	bool		enable_parallel_hash;
	bool		enable_partition_pruning;
	bool		enable_async_append;
} GucTable;
