# download url service

[DEPRECATED] Please use https://github.com/lblod/loket-download-url-service instead

Quick and dirty service to download urls that were provided.

Usage:

```
download:
    image: lblod/download-url-service
    links:
      - virtuoso:database
    volumes:
      - ./data/files:/share
    environment:
      CRON_PATTERN: "s m h dom mon dow"
```

