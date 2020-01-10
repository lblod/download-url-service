import { app, errorHandler, uuid } from 'mu';
import { waitForDatabase } from './database-utils';
import { getRemoteDataObjectByStatus,
         getRequestHeadersForRemoteDataObject,
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
       } from './queries';
import flatten from 'lodash.flatten';
import uniq from 'lodash.uniq';

import request from 'request';
import fs  from 'fs-extra';
import mime from 'mime-types';
import path from 'path';
import RootCas from 'ssl-root-cas/latest';
import https from 'https';
import bodyParser from 'body-parser';

const CACHING_MAX_RETRIES = 1; //parseInt(process.env.CACHING_MAX_RETRIES || 30);
const FILE_STORAGE = process.env.FILE_STORAGE || '/share';
const DEFAULT_EXTENSION = '.html';
const DEFAULT_CONTENT_TYPE = 'text/plain';

/***
 * Workaround for dealing with broken certificates configuration.
 * We downloaded the missing intermediate certificates
 */
const rootCas = RootCas.create();
const certificatesDir = '/app/certificates/';
fs.readdirSync(certificatesDir).forEach(file => {
  rootCas.addFile(certificatesDir + file);
});
https.globalAgent.options.ca = rootCas;

waitForDatabase(rescheduleTasksOnStart);

app.use( bodyParser.json( { type: function(req) { return /^application\/json/.test( req.get('content-type') ); } } ) );


app.get('/', function( req, res ) {
  res.send(`Welcome to the dowload url service.`);
});

app.post('/process-remote-data-objects', async function( req, res ){
  const delta = req.body;
  const remoteDataObjectUris = getRemoteDataObjectsFromDelta(delta);
  console.log(`Found ${remoteDataObjectUris.length} new remote data objects in the delta message`);
  processDownloads(remoteDataObjectUris);
  res.send({message: `Started.`});
});

app.use(errorHandler);

function getRemoteDataObjectsFromDelta(delta) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  const remoteDataObjectUris = inserts.filter( triple => {
    return triple.predicate.type == 'uri'
      && triple.predicate.value == 'http://www.w3.org/ns/adms#status'
      && triple.object.type == 'uri'
      && triple.object.value == 'http://lblod.data.gift/file-download-statuses/ready-to-be-cached';
  }).map( triple => triple.subject.value );
  return uniq(remoteDataObjectUris);
}

async function processDownloads(remoteDataObjectUris) {
  const remoteObjects = await getRemoteDataObjectByStatus(READY, remoteDataObjectUris);

  //create associated download events and lock in DB.
  for(let o of remoteObjects){
    let dlEventUri = await createDownloadEvent(o.subject.value);
    await updateStatus(o.subject.value, ONGOING);
    o.dlEventUri = dlEventUri;
  }

  for(let o of remoteObjects){
    try {
      await performDownloadTask(o, o.dlEventUri);
    }
    catch(error){
      handleDownloadTaskError(error, o, o.dlEventUri);
    }
  }
}

async function performDownloadTask(remoteObject, downloadEventUri){
  let requestHeaders = await getRequestHeadersForRemoteDataObject(remoteObject.subject);
  requestHeaders = requestHeaders
    .reduce((acc, h) => {
      acc[`${h.headerName.value}`] = h.headerValue.value;
      return acc;
    }, {});

  let downloadResult = await downloadFile(remoteObject, requestHeaders);
  let physicalFileUri = await associateCachedFile(downloadResult, remoteObject);
  await updateDownloadEventOnSuccess(downloadEventUri, physicalFileUri);
  await updateStatus(remoteObject.subject.value, SUCCESS);
  console.log(`Processed ${remoteObject.subject.value}, with url: ${remoteObject.url.value}`);
}

async function handleDownloadTaskError(error, remoteObject, downloadEventUri){
  console.error(`Error for ${remoteObject.subject.value} and task ${downloadEventUri}`);
  console.error(error);
  await updateStatus(downloadEventUri, FAILURE);
  scheduleRetryProcessing(remoteObject, downloadEventUri);
}

async function scheduleRetryProcessing(remoteObject, downloadEventUri){
  let downloadEvent = await getDownloadEvent(downloadEventUri);
  console.log(`Download event ${downloadEventUri} retried ${downloadEvent.numberOfRetries.value}/${CACHING_MAX_RETRIES} already`);
  if(downloadEvent.numberOfRetries.value >= CACHING_MAX_RETRIES){
    await updateStatus(remoteObject.subject.value, FAILURE);
    await updateStatus(downloadEventUri, FAILURE);
    console.log(`Stopping retries for ${remoteObject.subject.value} and task ${downloadEventUri})`);
    return;
  }

  let waitTime = calcTimeout(parseInt(downloadEvent.numberOfRetries.value));
  console.log(`Expecting next retry for ${remoteObject.subject.value} and task ${downloadEventUri} in about ${waitTime/1000} seconds`);
  setTimeout(async () => {
    try {
      console.log(`Retry for ${remoteObject.subject.value} and task ${downloadEventUri}`);
      await updateDownloadEvent(downloadEventUri, parseInt(downloadEvent.numberOfRetries.value) + 1, ONGOING);
      await performDownloadTask(remoteObject, downloadEventUri);
    }
    catch(error){
      handleDownloadTaskError(error, remoteObject, downloadEventUri);
    }
  }, waitTime);

}

async function rescheduleTasksOnStart(){
  let remoteObjects = await getRemoteDataObjectByStatus(ONGOING);
  for(let o of remoteObjects){
    try {
      await scheduleRetryProcessing(o, o.downloadEventUri.value);
    }
    catch(error){
      //if rescheduling fails, we consider there is something really broken...
      console.log(`Fatal error for ${o.subject.value}`);
      await updateStatus(o.subject.value, FAILURE);
    }
  }
};

function calcTimeout(x){
  //expected to be milliseconds
  return Math.round(Math.exp(0.3 * x + 10)); //I dunno I just gave it a shot
}

/**
 * Downloads the resource and takes care of errors.
 * Throws exception on failed download.
 */
async function downloadFile (remoteObject, headers) {

  return new Promise((resolve, reject) => {

    const url = remoteObject.url.value;

    const requestBody = {url};

    if(Object.keys(headers).length > 0){
      requestBody['headers'] = headers;
    }

    let r = request(requestBody);

    r.on('response', (resp) => {
      //check things about the response here.
      const code = resp.statusCode;

      //Note: by default, redirects are followed :-)
      if (200 <= code && code < 300) {
        //--- Status: OK
        //--- create file attributes
        let extension = getExtensionFrom(resp.headers);
        let bareName = uuid();
        let physicalFileName = [bareName, extension].join('.');
        let localAddress = path.join(FILE_STORAGE, physicalFileName);

        //--- write the file
        r.pipe(fs.createWriteStream(localAddress))
          .on('error', err => {
            //--- We need to clean up on error during file writing
            console.log (`${localAddress} failed writing to disk, cleaning up...`);
            cleanUpFile(localAddress);
            reject({resource: remoteObject, error: err});
          })
          .on('finish', () => {
            resolve({
                  resource: remoteObject,
                  result: resp,
                  cachedFileAddress: localAddress,
                  cachedFileName: physicalFileName,
                  bareName: bareName,
                  extension: extension
            });
          });
      }
      else {
        //--- NO OK
        reject({ resource: remoteObject, result: resp, error: `Response code http ${code}` });
      }
    });

    r.on('error', (err) => {
      console.error("Error while downloading a remote resource:");
      console.error(`  remote resource: ${remoteObject.subject.value}`);
      console.error(`  remote url: ${url}`);
      console.error(`  error: ${err}`);
      reject({resource: remoteObject, error: err});
    });
  });
}

/**
 * Creates an association between the cached file and the original FileAddress in the database
 */
async function associateCachedFile(downloadResult, remoteDataObjectQueryResult) {

  const uri = downloadResult.resource.subject.value;
  const name = downloadResult.cachedFileName;
  const extension = downloadResult.extension;
  const date = Date.now();

  //--- get the file's size
  const stats = fs.statSync(downloadResult.cachedFileAddress);
  const fileSize = stats.size;

  //--- read data from HTTP response headers
  const headers = downloadResult.result.headers;
  const contentType = getContentTypeFrom(headers);

  try {
    //create the physical file
    let physicalUri = 'share://' + downloadResult.cachedFileName; //we assume filename here
    let resultPhysicalFile = await createPhysicalFileDataObject(physicalUri,
                                                                remoteDataObjectQueryResult.subject.value,
                                                                name,
                                                                contentType,
                                                                fileSize,
                                                                extension,
                                                                date);
    return physicalUri;
  }
  catch (err) {
    console.error('Error while associating a downloaded file to a FileAddress object');
    console.error(err);
    console.error(`  downloaded file: ${downloadResult.cachedFileAddress}`);
    console.error(`  FileAddress object: ${uri}`);
    throw err;
  }
}

/**
 * Deletes a file.
 * Is intended to be used for deleting orphant files after a failure.
 * @param {string} path Local path to a file
 */
function cleanUpFile(path){
  if(fs.existsSync(path)){
    fs.unlinkSync(path);
  }
}

/**
 * Parses response headers to get the file content-type
 *
 * @param {array} headers HTML response header
 */
function getContentTypeFrom(headers) {
  return headers['content-type'] || DEFAULT_CONTENT_TYPE;
}

/**
 * Parses response headers to get the file extension
 *
 * @param {array} headers HTML response header
 */
function getExtensionFrom(headers) {
  const mimeType = headers['content-type'];
  return mime.extension(mimeType) || DEFAULT_EXTENSION;
}
