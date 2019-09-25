# download url service
Service responsible for processing of nfo:RemoteDataObject with adms:status set to <http://lblod.data.gift/download-url-statuses/ready-to-be-cached>

Service will download the associated URL as file.

When finished, nfo:RemoteDataObject with adms:status will be set to <http://lblod.data.gift/file-download-statuses/success>
When failed after capped retries: <http://lblod.data.gift/file-download-statuses/failure>

## usage

### docker-compose.yml

```
version: '3.4'
services:
    download-url-service:
        image: lblod/download-url-service
        links:
          - database:database
        volumes:
          - ./data/files:/share
        environment:
          DEFAULT_GRAPH: "http://mu.semte.ch/graphs/public"
          PING_DB_INTERVAL: "2"
          CACHING_MAX_RETRIES: "300"
          FILE_STORAGE: "/share"
```
The environment variables are shown with their default values.


### api

```
curl -X POST http://localhost/process-remote-data-objects
```
Will trigger the job.
