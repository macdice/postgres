# Copyright (c) 2024, PostgreSQL Global Development Group

tests += {
  'name': 'encoding',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'cluster_encoding',
    ],
    'regress_args': ['--no-locale', '--encoding=UTF8'],
    'runningcheck': false,
  },
}
