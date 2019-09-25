import { uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime } from 'mu';
import { querySudo as query } from '@lblod/mu-auth-sudo';

const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/graphs/public';
const READY = 'http://lblod.data.gift/file-download-statuses/ready-to-be-cached';
const ONGOING = 'http://lblod.data.gift/file-download-statuses/ongoing';
const SUCCESS = 'http://lblod.data.gift/file-download-statuses/success';
const FAILURE = 'http://lblod.data.gift/file-download-statuses/failure';

/**
 * get remote data objects
 * @param {String} status uri
 */
async function getRemoteDataObjectByStatus(status) {
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

    SELECT DISTINCT ?subject ?url ?uuid ?downloadEventUri WHERE{
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ?subject a nfo:RemoteDataObject;
                 mu:uuid ?uuid;
                 nie:url ?url;
                 adms:status ${sparqlEscapeUri(status)}.
         OPTIONAL { ?downloadEventUri nuao:involves ?subject }
      }
    }
  `;

  let result = await query(q);
  return  result.results.bindings || [];
};

async function updateStatus(uri, newStatusUri){
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>

    DELETE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status.
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status.
      }
    }
    ;
    INSERT DATA{
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ${sparqlEscapeUri(newStatusUri)}.
      }
    }
  `;
  await query(q);
}

async function createDownloadEvent(remoteDataObjectUri){
  let sUuid = uuid();
  let subject = `http://lblod.data.gift/download-events/${sUuid}`;
  let created = Date.now();

  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    ndo: <http://oscaf.sourceforge.net/ndo.html#>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(subject)} a task:Task;
                                                a ndo:DownloadEvent;
                                                mu:uuid ${sparqlEscapeString(sUuid)};
                                                adms:status ${sparqlEscapeUri(ONGOING)};
                                                task:numberOfRetries ${sparqlEscapeInt(0)};
                                                dct:created ${sparqlEscapeDateTime(created)};
                                                dct:modified ${sparqlEscapeDateTime(created)};
                                                dct:creator <http://lblod.data.gift/services/download-url-service>;
                                                nuao:involves ${sparqlEscapeUri(remoteDataObjectUri)}.
      }
    }
  `;
  let result = await query(q);
  return subject;
}

async function getDownloadEvent(subjectUri){
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    ndo: <http://oscaf.sourceforge.net/ndo.html#>

    SELECT DISTINCT ?subject ?uuid ?status ?created ?modified ?numberOfRetries ?involves WHERE{
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        VALUES ?subject { ${sparqlEscapeUri(subjectUri)} }.
        ?subject a ndo:DownloadEvent;
                     mu:uuid ?uuid;
                     adms:status ?status;
                     task:numberOfRetries ?numberOfRetries;
                     dct:created ?created;
                     dct:modified ?modified;
                     nuao:involves ?involves.
      }
    }
  `;
  let result = await query(q);
  return  (result.results.bindings || [])[0];
}

async function updateDownloadEvent(uri, numberOfRetries, newStatusUri){
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>

    DELETE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status;
                                task:numberOfRetries ?numberOfRetries.
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status;
                                task:numberOfRetries ?numberOfRetries.

      }
    }
    ;
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ${sparqlEscapeUri(newStatusUri)};
                                task:numberOfRetries ${sparqlEscapeInt(numberOfRetries)}.
      }
    }
  `;
  await query(q);
}

async function updateDownloadEventOnSuccess(uri, fileUri){
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

    DELETE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status.
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status.
      }
    }
    ;
    INSERT DATA{
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ${sparqlEscapeUri(SUCCESS)};
                                nuao:involves ${sparqlEscapeUri(fileUri)}.
      }
    }
  `;
  await query(q);
}

async function createPhysicalFileDataObject(fileObjectUri, dataSourceUri, name, type, fileSize, extension, created){
  if(!fileObjectUri.startsWith('share://')) throw Error('File URI should start with share://');

  const uid = uuid();
  let q = `
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    dbpedia: <http://dbpedia.org/ontology/>
    PREFIX    ndo: <http://oscaf.sourceforge.net/ndo.html#>
    PREFIX    dct: <http://purl.org/dc/terms/>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(fileObjectUri)} a nfo:FileDataObject;
              a nfo:LocalFileDataObject;
              nfo:fileName ${sparqlEscapeString(name)};
              nie:dataSource ${sparqlEscapeUri(dataSourceUri)};
              ndo:copiedFrom ${sparqlEscapeUri(dataSourceUri)};
              mu:uuid ${sparqlEscapeString(uid)};
              dct:format ${sparqlEscapeString(type)};
              nfo:fileSize ${sparqlEscapeInt(fileSize)};
              dbpedia:fileExtension ${sparqlEscapeString(extension)};
              nfo:fileCreated ${sparqlEscapeDate(created)}.
      }
    }
  `;
  return await query( q );
};

export { getRemoteDataObjectByStatus,
         updateStatus,
         createDownloadEvent,
         getDownloadEvent,
         updateDownloadEvent,
         createPhysicalFileDataObject,
         updateDownloadEventOnSuccess,
         READY,
         ONGOING,
         SUCCESS,
         FAILURE
       }
