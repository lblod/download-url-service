import { query, uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime } from 'mu';

/**
 * SPARQL constants
 */
const UUID_URI = '<http://mu.semte.ch/vocabularies/core/uuid>';
const EXT_PREFIX = 'ext: <http://mu.semte.ch/vocabularies/ext/>';
const NFO_PREFIX = 'nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>';
const NIE_PREFIX = 'nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>';
const TOEZICHT_PREFIX = 'toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>';
const ADMS_PREFIX = 'adms: <http://www.w3.org/ns/adms#>';
const DCT_PREFIX = 'dct: <http://purl.org/dc/terms/>';
const STATUS_RESOURCES_PATH = "http://data.lblod.info/file-address-cache-statuses";
const FILE_RESOURCES_PATH = "http://data.lblod.info/files/";
/**
 * States of a FileAddress object
 * PENDING : is being downloaded
 * FAILED : last download has failed
 * CACHED : has been successfully cached
 * DEAD : has been tried for the maximum allowed times
 */
const PENDING = 'pending';
const FAILED = 'failed';
const CACHED = 'cached';
const DEAD = 'dead';
const READY_TO_BE_CACHED = 'ready-to-be-cached';

const MAX_PENDING_TIME_IN_SECONDS = process.env.MAX_PENDING_TIME_IN_SECONDS || 3600; //--- 1 hour as default

/**
 * Queries the database for elligible FileAddress objects
 * These include FileAddress objects which either have not been tried before or have failed.
 * Also, items which have been downloading for a long time are included.
 *
 * @param {number} caching_max_retries The maximum number of times an item shall be tried
 */
async function getFileAddressToDo ( caching_max_retries ) {
  //--- get a list of all failed FileAddress objects
  let q = `
    PREFIX ${EXT_PREFIX}
    PREFIX ${TOEZICHT_PREFIX}
    PREFIX ${ADMS_PREFIX}

    SELECT DISTINCT ?uri ?url ?timesTried ?statusLabel {
      ?uri a ext:FileAddress ;
           ext:fileAddressCacheStatus ?statusUri .

      ?uri ext:fileAddress ?url .

      OPTIONAL {
        ?statusUri ext:fileAddressCacheStatusLabel ?statusLabel .
      }

      OPTIONAL {
        ?statusUri ext:fileAddressCacheStatusTimesRetried ?timesTried .
      }

      OPTIONAL {
        ?uri ext:fileAddressCacheStatus ?statusUri .
        ?statusUri ext:fileAddressCacheStatusLabel ?statusLabel .
      }

      OPTIONAL {
        ?uri ext:fileAddressCacheStatus ?statusUri .
        ?statusUri ext:fileAddressCacheStatusInitiationTime ?timeInitiated .
      }

      BIND (IF (BOUND(?timeInitiated), NOW() - ?timeInitiated, 0) as ?elapsed) .

      FILTER (
        (!BOUND(?statusLabel)
        ||
        ?statusLabel = ${sparqlEscapeString(READY_TO_BE_CACHED)}
        ||
        ?statusLabel = ${sparqlEscapeString(FAILED)}
        ||
        (?statusLabel = ${sparqlEscapeString(PENDING)} && ?elapsed > ${MAX_PENDING_TIME_IN_SECONDS}))
      )
    }
  `;

  let qResults = [];
  try {
    qResults = await query(q);
  } catch (err) {
    console.log(`Error while querying the list of fileAddresses`)
    console.log(err);
  }

  return qResults.results.bindings || [];
};

/**
 *
 * @param {uri} uri The FileAddress uri
 * @param {string} statusLabel New status's label
 * @param {number} responseCode If we are setting the status because of some server response, this is that rsponse's code
 * @param {number} timesTried Keeps track of how many times this item has been tried up to now
 */
async function setStatus (uri, statusLabel, responseCode = null, timesTried = 0) {

  console.log(`Setting ${statusLabel} status for ${uri}`);

  const uid = uuid();

  let q = `
    PREFIX ${EXT_PREFIX}

    DELETE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} ext:fileAddressCacheStatus ?status .
        ?status ?p ?o .
      }
    }
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} ext:fileAddressCacheStatus ?status .
        ?status ?p ?o .
      }
    }

    ;

    INSERT {
      GRAPH ?g {
        ?statusUri a
            ext:FileAddressCacheStatus ;
            ext:fileAddressCacheStatusLabel ${sparqlEscapeString(statusLabel)} ;
            ${responseCode != null ? `ext:fileAddressCacheStatusHttpStatus ${sparqlEscapeInt(responseCode)};` : ''}
            ext:fileAddressCacheStatusTimesRetried ${sparqlEscapeInt(timesTried)} ;
            ext:fileAddressCacheStatusInitiationTime ${sparqlEscapeDateTime(Date.now())} ;
            ${UUID_URI} ${sparqlEscapeString(uid)} .
        ${sparqlEscapeUri(uri)} ext:fileAddressCacheStatus ?statusUri .
      }
    }
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} a ext:FileAddress.
      }
      BIND(IRI(${sparqlEscapeString([STATUS_RESOURCES_PATH, statusLabel, uid].join('/'))}) as ?statusUri).
    }
  `;

  try {
    return await query( q );
  }
  catch (err) {
    console.log(`Error while setting ${statusLabel} status`);
    console.log(` resource: ${uri}`);
    console.log(` error: ${err}`);
    throw err;
  }
};

async function createVirtualFileDataObject (fileObjectUri, fileAddressUri, name, type, fileSize, extension, created){
  const uid = uuid();
  let q = `
    PREFIX ${EXT_PREFIX}
    PREFIX ${NFO_PREFIX}
    PREFIX ${NIE_PREFIX}
    PREFIX ${DCT_PREFIX}

    INSERT {
      GRAPH <http://mu.semte.ch/graphs/public> {
        # make a file resource
        ${sparqlEscapeUri(fileObjectUri)} a
            nfo:FileDataObject;
            nfo:fileName ${sparqlEscapeString(name)};
            dct:format ${sparqlEscapeString(type)};
            nfo:fileSize ${sparqlEscapeInt(fileSize)};
            dbpedia:fileExtension ${sparqlEscapeString(extension)};
            nfo:fileCreated ${sparqlEscapeDate(created)};
            ${UUID_URI} ${sparqlEscapeString(uid)}.

        # associate it to our original FileAddress object
        ${sparqlEscapeUri(fileObjectUri)} nie:dataSource  ${sparqlEscapeUri(fileAddressUri)}.

        #HACK for the sprintf issue
        ${sparqlEscapeUri(fileAddressUri)} ?p ?o.
      }
    }
  `;
  return await query( q );
};

async function createPhysicalFileDataObject (fileObjectUri, dataSourceUri, name, type, fileSize, extension, created){
  //TODO: merge with pervious query
  const uid = uuid();
  let q = `
    PREFIX ${EXT_PREFIX}
    PREFIX ${NFO_PREFIX}
    PREFIX ${NIE_PREFIX}
    PREFIX ${DCT_PREFIX}

    INSERT {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${sparqlEscapeUri(fileObjectUri)} a nfo:FileDataObject;
              nfo:fileName ${sparqlEscapeString(name)};
              nie:dataSource ${sparqlEscapeUri(dataSourceUri)};
              ${UUID_URI} ${sparqlEscapeString(uid)};
              dct:format ${sparqlEscapeString(type)};
              nfo:fileSize ${sparqlEscapeInt(fileSize)};
              dbpedia:fileExtension ${sparqlEscapeString(extension)};
              nfo:fileCreated ${sparqlEscapeDate(created)}.

        #HACK for the sprintf issue
        ${sparqlEscapeUri(dataSourceUri)} ?p ?o.
      }
    }
  `;
  return await query( q );
};

export { createVirtualFileDataObject, createPhysicalFileDataObject, getFileAddressToDo, setStatus, PENDING, FAILED, CACHED, DEAD, FILE_RESOURCES_PATH}
