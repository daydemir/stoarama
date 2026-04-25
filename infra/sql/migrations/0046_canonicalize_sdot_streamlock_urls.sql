UPDATE streams
SET source_url = replace(
      source_url,
      'https://61e0c5d388c2e.streamlock.net:443/',
      'https://61e0c5d388c2e.streamlock.net/'
    ),
    updated_at = now()
WHERE source_url LIKE 'https://61e0c5d388c2e.streamlock.net:443/%';
