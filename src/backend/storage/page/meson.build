# Copyright (c) 2022-2025, PostgreSQL Global Development Group

backend_sources += files(
  'bufpage.c',
  'itemptr.c',
)

checksum = static_library('checksum',
  files('checksum.c'),
  dependencies: [backend_code],
  c_args: [vectorize_cflags, unroll_loops_cflags],
  kwargs: internal_lib_args,
)
backend_link_with += checksum
