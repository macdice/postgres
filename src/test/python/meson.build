# Copyright (c) 2023, PostgreSQL Global Development Group

tests += {
  'name': 'python',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'pytest': {
	'requirements': meson.current_source_dir() / 'requirements.txt',
    'tests': [
      './client',
      './server',
      './test_internals.py',
      './test_pq3.py',
    ],
    'env': {
      'with_oauth': oauth_library,

      # Point to the default database; the tests will create their own databases
      # as needed.
      'PGDATABASE': 'postgres',

      # Avoid the need for a Rust compiler on platforms without prebuilt wheels
      # for pyca/cryptography.
      'CRYPTOGRAPHY_DONT_BUILD_RUST': '1',
    },
  },
}
