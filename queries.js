import { uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';

const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/graphs/public';
const READY = 'http://lblod.data.gift/file-download-statuses/ready-to-be-cached';
const ONGOING = 'http://lblod.data.gift/file-download-statuses/ongoing';
const SUCCESS = 'http://lblod.data.gift/file-download-statuses/success';
const FAILURE = 'http://lblod.data.gift/file-download-statuses/failure';
const BASIC_AUTH = 'https://www.w3.org/2019/wot/security#BasicSecurityScheme';
const OAUTH2 = 'https://www.w3.org/2019/wot/security#OAuth2SecurityScheme';

/**
 * get remote data objects
 * @param {String} status uri
 */
async function getRemoteDataObjectByStatus(status, uris = []) {
  let subjectValues = '';
  if (uris.length) {
    subjectValues = `
      VALUES ?subject {
        ${uris.map(sparqlEscapeUri).join('\n')}
      }
    `;
  }

  const q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

    SELECT DISTINCT ?subject ?url ?uuid ?downloadEventUri
    WHERE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ?subject a nfo:RemoteDataObject .
        ${subjectValues}
        ?subject mu:uuid ?uuid;
                 nie:url ?url;
                 adms:status ${sparqlEscapeUri(status)}.
        OPTIONAL { ?downloadEventUri nuao:involves ?subject }
      }
    }
  `;

  const result = await query(q);
  return result.results.bindings || [];
};

async function getRequestHeadersForRemoteDataObject(subject) {
  const q = `
    PREFIX http: <http://www.w3.org/2011/http#>
    PREFIX rpioHttp: <http://redpencil.data.gift/vocabularies/http/>

    SELECT DISTINCT ?header ?headerValue ?headerName WHERE {
     GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
       ${sparqlEscapeUri(subject.value)} rpioHttp:requestHeader ?header.
       ?header http:fieldValue ?headerValue.
       ?header http:fieldName ?headerName.
     }
    }
  `;
  await query(q);
  const result = await query(q);
  return result.results.bindings || [];
};

async function getCredentialsTypeForRemoteDataObject(subject) {
  const q = `
    PREFIX dgftSec: <http://lblod.data.gift/vocabularies/security/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?securityConfigurationType WHERE {
      GRAPH ?g {
        ?submission nie:hasPart ${sparqlEscapeUri(subject.value)} ;
          dgftSec:targetAuthenticationConfiguration ?configuration .
        ?configuration dgftSec:securityConfiguration/rdf:type ?securityConfigurationType .
      }
    }
  `;
  await query(q);
  const result = await query(q);
  return result.results.bindings[0] ? result.results.bindings[0].securityConfigurationType.value : null;
};

async function getBasicCredentialsForRemoteDataObject(subject) {
  const q = `
    PREFIX dgftSec: <http://lblod.data.gift/vocabularies/security/>
    PREFIX meb: <http://rdf.myexperiment.org/ontologies/base/>
    PREFIX muAccount: <http://mu.semte.ch/vocabularies/account/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT DISTINCT ?user ?pass WHERE {
      GRAPH ?g {
        ?submission nie:hasPart ${sparqlEscapeUri(subject.value)} ;
          dgftSec:targetAuthenticationConfiguration ?configuration .
        ?configuration dgftSec:secrets ?secrets .
        ?secrets meb:username ?user ;
          muAccount:password ?pass .
      }
    }
  `;
  await query(q);
  const result = await query(q);
  return result.results.bindings[0] || null;
};

async function getOauthCredentialsForRemoteDataObject(subject) {
  const q = `
    PREFIX dgftSec: <http://lblod.data.gift/vocabularies/security/>
    PREFIX oauthSession: <http://kanselarij\.vo\.data\.gift/vocabularies/oauth-2\.0-session/>
    PREFIX security: <https://www.w3.org/2019/wot/security#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT DISTINCT ?clientId ?clientSecret ?accessTokenUri ?resource WHERE {
      GRAPH ?g {
        ?submission nie:hasPart ${sparqlEscapeUri(subject.value)} ;
          dgftSec:targetAuthenticationConfiguration ?configuration .
        ?configuration dgftSec:secrets ?secrets ;
          dgftSec:securityConfiguration ?securityConfiguration .
        ?secrets oauthSession:clientId ?clientId ;
          oauthSession:clientSecret ?clientSecret .
        ?securityConfiguration oauthSession:resource ?resource ;
          security:token ?accessTokenUri .
      }
    }
  `;
  await query(q);
  const result = await query(q);
  return result.results.bindings[0] || null;
};

async function updateStatus(uri, newStatusUri) {
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
  await update(q);
}

async function createDownloadEvent(remoteDataObjectUri) {
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
  let result = await update(q);
  return subject;
}

async function getDownloadEvent(subjectUri) {
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
  return (result.results.bindings || [])[0];
}

async function updateDownloadEvent(uri, numberOfRetries, newStatusUri) {
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
  await update(q);
}

async function updateDownloadEventOnSuccess(uri, fileUri) {
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
  await update(q);
}

async function createPhysicalFileDataObject(fileObjectUri, dataSourceUri, name, type, fileSize, extension, created) {
  if (!fileObjectUri.startsWith('share://')) throw Error('File URI should start with share://');

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
  return await update(q);
};

async function saveHttpStatusCode(remoteUrl, statusCode) {
  let q = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    DELETE {
      GRAPH ?g {
        ?url ext:httpStatusCode ?code.
      }
    }
    WHERE {
      BIND(${sparqlEscapeUri(remoteUrl)} as ?url)
      BIND(${sparqlEscapeUri(DEFAULT_GRAPH)} as ?g)

      GRAPH ?g {
        ?url ext:httpStatusCode ?code.
      }
    }
    ;
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(remoteUrl)} ext:httpStatusCode ${sparqlEscapeInt(statusCode)}.
      }
    }
  `;
  await update(q);
}

async function saveCacheError(remoteUrl, error) {
  let q = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    DELETE {
      GRAPH ?g {

        ?url ext:cacheError ?error.
      }
    }
    WHERE {
      BIND(${sparqlEscapeUri(remoteUrl)} as ?url)
      BIND(${sparqlEscapeUri(DEFAULT_GRAPH)} as ?g)

      GRAPH ?g {
        ?url ext:cacheError ?code.
      }
    }
    ;
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(remoteUrl)} ext:cacheError ${sparqlEscapeString(error.toString())}.
      }
    }
  `;
  await update(q);
}

async function getRemoteDataObject(uuid) {
  let q = `
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>

    SELECT DISTINCT ?s WHERE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ?s mu:uuid ${sparqlEscapeString(uuid)}.
      }
    }
  `;
  let result = await query(q);
  return result.results.bindings ? result.results.bindings[0].s.value : null;
}

async function deleteCredentials(remoteDataObject, credentialsType) {

  if (!credentialsType)
    credentialsType = await getCredentialsTypeForRemoteDataObject(remoteDataObject.subject);

  switch (credentialsType) {
    case BASIC_AUTH:
      await update(`
      PREFIX dgftSec: <http://lblod.data.gift/vocabularies/security/>
      PREFIX meb: <http://rdf.myexperiment.org/ontologies/base/>
      PREFIX muAccount: <http://mu.semte.ch/vocabularies/account/>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

      DELETE {
        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets meb:username ?user ;
            muAccount:password ?pass .
        }
      } WHERE {
        GRAPH ?g {
          ?submission nie:hasPart ${sparqlEscapeUri(remoteDataObject.subject.value)} ;
            dgftSec:targetAuthenticationConfiguration ?configuration .
          ?configuration dgftSec:secrets ?secrets .
          ?secrets meb:username ?user ;
            muAccount:password ?pass .
        }
      }
    `);
      break;
    case OAUTH2:
      await update(`
      PREFIX dgftSec: <http://lblod.data.gift/vocabularies/security/>
      PREFIX oauthSession: <http://kanselarij\.vo\.data\.gift/vocabularies/oauth-2\.0-session/>
      PREFIX security: <https://www.w3.org/2019/wot/security#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

      DELETE {
        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets oauthSession:clientId ?clientId ;
            oauthSession:clientSecret ?clientSecret .
        }
      } WHERE {
        GRAPH ?g {
          ?submission nie:hasPart ${sparqlEscapeUri(remoteDataObject.subject.value)} ;
            dgftSec:targetAuthenticationConfiguration ?configuration .
          ?configuration dgftSec:secrets ?secrets .
          ?secrets oauthSession:clientId ?clientId ;
            oauthSession:clientSecret ?clientSecret .
        }
      }
    `);
      break;
    default:
      return false;
  }
}

export {
  getRemoteDataObjectByStatus,
  getRequestHeadersForRemoteDataObject,
  getCredentialsTypeForRemoteDataObject,
  getBasicCredentialsForRemoteDataObject,
  getOauthCredentialsForRemoteDataObject,
  deleteCredentials,
  updateStatus,
  createDownloadEvent,
  getDownloadEvent,
  updateDownloadEvent,
  createPhysicalFileDataObject,
  updateDownloadEventOnSuccess,
  saveHttpStatusCode,
  saveCacheError,
  getRemoteDataObject,
  READY,
  ONGOING,
  SUCCESS,
  FAILURE,
  BASIC_AUTH,
  OAUTH2
};
