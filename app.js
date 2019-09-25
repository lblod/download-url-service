import { app, errorHandler, uuid } from 'mu';
import { getRemoteDataObjectByStatus,
         updateStatus,
         createDownloadEvent,
         getDownloadEvent,
         getDownloadEventsByStatus,
         updateDownloadEvent,
         createPhysicalFileDataObject,
         updateDownloadEventOnSuccess,
         READY,
         ONGOING,
         SUCCESS,
         FAILURE
       } from './queries';

import request from 'request';
import fs  from 'fs-extra';
import mime from 'mime-types';
import path from 'path';
import RootCas from 'ssl-root-cas/latest';
import https from 'https';

const CACHING_MAX_RETRIES = parseInt(process.env || {}).CACHING_MAX_RETRIES || 300;
const FILE_STORAGE = (process.env || {}).FILE_STORAGE || '/share';
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

app.get('/', function( req, res ) {
  res.send(`Welcome to the dowload url service.`);
});

app.post('/process-remote-data-objects', async function( req, res ){
  process();
  res.send({message: `Started.`});
});

app.use(errorHandler);

async function process() {
  const remoteObjects = await getRemoteDataObjectByStatus(READY);

  //lock em first in DB
  for(let o of remoteObjects){
    await updateStatus(o.subject.value, ONGOING);
  }

  //start processing
  for(let o of remoteObjects){
    let dlEventUri = await createDownloadEvent(o.subject.value);
    let downloadResult = await downloadFile(o);
    //TODO: handle 500 (so not successful)
    let physicalFileUri = await associateCachedFile(downloadResult, o);
    await updateDownloadEventOnSuccess(dlEventUri, physicalFileUri);
    await updateStatus(o.subject.value, SUCCESS);
    console.log(`Processed ${o.subject.value}, with url: ${o.url.value}`);
  }

  // //--- start the process of downloading the resources
  // const promises = fileAddresses.map( async (fileAddress) => {

  //   const uri = fileAddress.uri.value;
  //   const url = fileAddress.url.value;
  //   const timesTried = fileAddress.hasOwnProperty('timesTried') ? parseInt(fileAddress.timesTried.value) : 0;

  //   let downloadResult = null;
  //   let associationResult = null;

  //   console.log(`Enqueuing ${url}`);

  //   try {
  //     //--- setting fileAddress's status to 'downloading' prevents us from
  //     //--- redownloading a resource in case it's download
  //     //--- takes longer than our iteration interval
  //     await setStatus (uri, PENDING, null, timesTried);
  //   }
  //   catch (err) {
  //     return;
  //   }

  //   try {
  //     //--- download the content of fileAddress
  //     downloadResult = await downloadFile(fileAddress);
  //   }
  //   catch (err) {
  //     //--- A connection to the remote resource was not established
  //     //--- update the cachedStatus of the fileAddress to either FAILED or DEAD
  //     await setStatus(uri, getStatusLabelFor(timesTried), null, timesTried + 1);
  //     return;
  //   }

  //   if (downloadResult.successful) {
  //     try {
  //       console.log(`Associating ${uri}`);
  //       console.log(`            ${url}`);
  //       //--- associate the downloaded file to the fileAddress
  //       associationResult = await associateCachedFile(downloadResult);
  //     }
  //     catch (err) {
  //       //--- The file has been successfuly deleted but it could not be associated
  //       //--- with the FileAddress object in the database, maybe for some database error.
  //       //--- We need to clean up
  //       cleanUpFile(downloadResult.cachedFileAddress);
  //       //--- Since this failure was not due to the remote server, we will try it again
  //       //--- So, we don't inrease the timesTried value
  //       await setStatus(uri, FAILED, null, timesTried);
  //       return;
  //     }
  //   } else {
  //     //--- Due to an error on the remote resource side, the file could not be downloaded
  //     //--- update the cachedStatus of the fileAddress to either FAILED or DEAD
  //     await setStatus(uri, getStatusLabelFor(timesTried), parseInt(downloadResult.result.statusCode), timesTried + 1);
  //     return;
  //   }

  //   //--- File was successfuly downloaded and cached
  //   //--- update the cachedStatus of the fileAddress to CACHED
  //   await setStatus(uri, CACHED, parseInt(downloadResult.result.statusCode), timesTried + 1);
  //   console.log (`${url} is cached successfuly`);
  //});
}

/**
 * Downloads the resource and takes care of errors
 *
 * @param { uri, url, timesTried, statusLabel } fileAddress The necessary data from the FileAddress object
 */
async function downloadFile (remoteObject) {

  return new Promise((resolve, reject) => {

    const url = remoteObject.url.value;
    let r = request(url);

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
                  successful: true,
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
        resolve({ successful: false, resource: remoteObject, result: resp });
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
