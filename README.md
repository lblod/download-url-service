# download url service

# loket-download-url-service

A microservice that periodically looks in the database for urls that are ready to be cached and tries to download and store their content locally, if not already stored. You can force it to run immediately by visiting /checkurls subroute.

```
  FILE_STORAGE
    The local storage of files

  CACHING_MAX_RETRIES
    How many times will the service try to download a resource before considering it as failed.

  CACHING_CRON_PATTERN
    The time interval of service's re-execution.
```

## Installation

To add the service to your stack, add the following snippet to docker-compose.yml:

```
download:
    image: lblod/download-url-service:0.0.3
    volumes:
      - ./data/files:/data/files             # The right hand side of : must be in sync with FILE_STORAGE
    restart: always
    logging: *default-logging
    environment:
      CACHING_MAX_RETRIES: 300
      CACHING_CRON_PATTERN: '0 */15 * * * *' # run every quarter
      MAX_PENDING_TIME_IN_SECONDS: 7200      # set as you wish
      NODE_ENV: "development"                # set as you wish
      FILE_STORAGE: '/data/files'            # set as you wish
```
