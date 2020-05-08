# download-url-service

Microservice downloading a local copy of a remote file by URL.

## Getting started
### Add the service to a stack
Add the following snippet in your `docker-compose.yml`:

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
          CACHING_MAX_RETRIES: "30"
          FILE_STORAGE: "/share"
```

The environment variables are shown with their default values.

Configure the [delta-notifier](https://github.com/mu-semtech/delta-notifier) to trigger the service by adding the following rules in the delta configuration:

```
  {
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status'
      },
      object: {
        type: 'uri',
        value: 'http://lblod.data.gift/file-download-statuses/ready-to-be-cached'
      }
    },
    callback: {
      method: 'POST'
      url: 'http://download-url/process-remote-data-objects',
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  }
```

## Reference
### Configuration
The following environment variales can be configured:
* `DEFAULT_GRAPH`: graph to write the download event and file to
* `CACHING_MAX_RETRIES`: number of attempts to download a file before the download event transitions to a failure state
* `FILE_STORAGE`: absolute path inside the container in which the downloaded files must be stored
* `PING_DB_INTERVAL`: interval in seconds to ping the database on startup of the service

### Model
The service is triggered by updates of resources of type `nfo:RemoteDataObject` of which the status is updated to `http://lblod.data.gift/file-download-statuses/ready-to-be-cached`. It will download the associated URL (`nie:url`) as file.

The download service will create a download event (`ndo:DownloadEvent`) and a local file data object (`nfo:LocalFileDataObject`) on succesfull download of the remote resource. The properties of these resources are specified below.

The model of this service is compliant with the model of the [file-service](http://github.com/mu-semtech/file-service). Hence, the cached files can be downloaded using this service.

See also: http://oscaf.sourceforge.net/ndo.html#ndo:sec-file-downloads

#### Used prefixes
| Prefix       | URI                                                       |
|--------------|-----------------------------------------------------------|
| dct          | http://purl.org/dc/terms/                                 |
| adms         | http://www.w3.org/ns/adms#                                |
| prov         | http://www.w3.org/ns/prov#                                |
| task         | http://redpencil.data.gift/vocabularies/tasks/ |
| ndo          | http://oscaf.sourceforge.net/ndo.html# |
| nuao         | http://www.semanticdesktop.org/ontologies/2010/01/25/nuao# |

#### Download event
##### Class
`ndo:DownloadEvent`, `task:Task`
##### Properties
| Name       | Predicate        | Range            | Definition                                                                                                                          |
|------------|------------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| status     | `adms:status`    | `adms:Status`    | Status of the download                             |
| created    | `dct:created`    | `xsd:dateTime`   | Datetime of creation of the task                                                                                          |
| modified   | `dct:modified`   | `xsd:dateTime`   | Datetime on which the task was modified                                                                                             |
| creator    | `dct:creator`    | `rdf:Resource`   | Creator of the task, in this case the download-url-service `<http://lblod.data.gift/services/download-url-service>` |
| files      | `nuao:involves`   | `nfo:FileDataObject` | Files involved in the download event (remote file as well as local file) |
___
#### Remote file data object
Reflects the remote file that needs to be downloaded.
##### Class
`nfo:RemoteDataObject`
##### Properties
See data model of the [file service](https://github.com/mu-semtech/file-service#resources).
___
#### Local file data object
Reflects the local, downloaded copy of the remote file
##### Class
`nfo:FileDataObject`
##### Properties
See data model of the [file service](https://github.com/mu-semtech/file-service#resources).
___
#### File download statuses
The `adms:status` property of the `nfo:RemoteDataObject` reflects the status of the download. The following statuses are possible:

| Status | URI |
| --- | --- |
| Ready to be cached | http://lblod.data.gift/file-download-statuses/ready-to-be-cached |
| Ongoing | http://lblod.data.gift/file-download-statuses/ongoing |
| Success | http://lblod.data.gift/file-download-statuses/success |
| Failure | http://lblod.data.gift/file-download-statuses/failure |
