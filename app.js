import { app, query, errorHandler, uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate } from 'mu';
import { createVirtualFileDataObject, createPhysicalFileDataObject, getFileAddressToDo, setStatus, PENDING, FAILED, CACHED, DEAD, FILE_RESOURCES_PATH } from './queries';
import { CronJob } from 'cron';
import request from 'request';
import fs  from 'fs-extra';
import mime from 'mime-types';
import path from 'path';
import RootCas from 'ssl-root-cas/latest';
import https  from 'https';

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

/**
* Environment constants
*/
const CACHING_MAX_RETRIES = parseInt(process.env.CACHING_MAX_RETRIES) || 300;
const FILE_STORAGE = process.env.FILE_STORAGE || '/data/files';
const CRON_FREQUENCY = process.env.CACHING_CRON_PATTERN || '0 */15 * * * *';

/**
* Internal constants
*/
const DEFAULT_EXTENSION = '.html';
const DEFAULT_CONTENT_TYPE = 'text/plain';

app.get('/', function( req, res ) {
  res.send(`
Welcome to the dowload url service.

This service periodically looks for urls and tries to download and store their content locally, if not already stored. You can force it to run immediately by visiting /checkurls subroute. This service utilizes these environment variables:

  FILE_STORAGE
    The local storage of files

  CACHING_MAX_RETRIES
    How many times will the service try to download a resource before considering it as failed.

  CACHING_CRON_PATTERN
    The time interval of service's re-execution.`);
});

/**
 * This route can be used to force an immidiate run of the service
 */
app.get('/checkurls', async function( req, res ){
  fetchingJob();
  res.send(`Started. Repeating pattern: '${CRON_FREQUENCY}'`);
});

app.use(errorHandler);

new CronJob(CRON_FREQUENCY, async function() {
  console.log(`Download-url service triggered by cron job at ${new Date().toISOString()}`);
  await fetchingJob();
}, null, true);

/**
 * This function fetches the resources
 * from urls provided in the database
 * and caches them locally
 */
async function fetchingJob () {

  //--- get the list of resource urls
  const fileAddresses = await getFileAddressToDo(CACHING_MAX_RETRIES);

  //--- start the process of downloading the resources
  const promises = fileAddresses.map( async (fileAddress) => {

    const uri = fileAddress.uri.value;
    const url = fileAddress.url.value;
    const timesTried = fileAddress.hasOwnProperty('timesTried') ? parseInt(fileAddress.timesTried.value) : 0;

    let downloadResult = null;
    let associationResult = null;

    console.log(`Enqueuing ${url}`);

    try {
      //--- setting fileAddress's status to 'downloading' prevents us from
      //--- redownloading a resource in case it's download
      //--- takes longer than our iteration interval
      await setStatus (uri, PENDING, null, timesTried);
    }
    catch (err) {
      return;
    }

    try {
      //--- download the content of fileAddress
      downloadResult = await downloadFile(fileAddress);
    }
    catch (err) {
      //--- A connection to the remote resource was not established
      //--- update the cachedStatus of the fileAddress to either FAILED or DEAD
      await setStatus(uri, getStatusLabelFor(timesTried), null, timesTried + 1);
      return;
    }

    if (downloadResult.successful) {
      try {
        console.log(`Associating ${uri}`);
        console.log(`            ${url}`);
        //--- associate the downloaded file to the fileAddress
        associationResult = await associateCachedFile(downloadResult);
      }
      catch (err) {
        //--- The file has been successfuly deleted but it could not be associated
        //--- with the FileAddress object in the database, maybe for some database error.
        //--- We need to clean up
        cleanUpFile(downloadResult.cachedFileAddress);
        //--- Since this failure was not due to the remote server, we will try it again
        //--- So, we don't inrease the timesTried value
        await setStatus(uri, FAILED, null, timesTried);
        return;
      }
    } else {
      //--- Due to an error on the remote resource side, the file could not be downloaded
      //--- update the cachedStatus of the fileAddress to either FAILED or DEAD
      await setStatus(uri, getStatusLabelFor(timesTried), parseInt(downloadResult.result.statusCode), timesTried + 1);
      return;
    }

    //--- File was successfuly downloaded and cached
    //--- update the cachedStatus of the fileAddress to CACHED
    await setStatus(uri, CACHED, parseInt(downloadResult.result.statusCode), timesTried + 1);
    console.log (`${url} is cached successfuly`);
  });
}

/**
 * Decides on the label of this item's new status
 *
 * @param {number} times The number of times this resource has already been tried
 */
function getStatusLabelFor (times) {
  let val = times + 1 < CACHING_MAX_RETRIES ? FAILED : DEAD;
  return val;
}

/**
 * Makes a name for the local file in which the data is cached
 */
function makeFileName() {
  return uuid();
}

/**
 * Downloads the resource and takes care of errors
 *
 * @param { uri, url, timesTried, statusLabel } fileAddress The necessary data from the FileAddress object
 */
async function downloadFile (fileAddress) {

  return new Promise((resolve, reject) => {

    const uri = fileAddress.uri.value;
    const url = fileAddress.url.value;
    let r = request(url);

    r.on('response', (resp) => {
      //check things about the response here.
      const code = resp.statusCode;

      //Note: by default, redirects are followed :-)
      if (200 <= code && code < 300) {
        //--- Status: OK
        //--- create file attributes
        let extension = getExtensionFrom(resp.headers);
        let bareName = makeFileName();
        let physicalFileName = [bareName, extension].join('.');
        let localAddress = path.join(FILE_STORAGE, physicalFileName);

        //--- write the file
        r.pipe(fs.createWriteStream(localAddress))
          .on('error', err => {
            //--- We need to clean up on error during file writing
            console.log (`${localAddress} failed writing to disk, cleaning up...`);
            cleanUpFile(localAddress);
            reject({resource: fileAddress, error: err});
          })
          .on('finish', () => {
            resolve({
                  successful: true,
                  resource: fileAddress,
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
        resolve({ successful: false, resource: fileAddress, result: resp });
      }
    });

    r.on('error', (err) => {
      console.log("Error while downloading a remote resource:");
      console.log(`  remote resource: ${uri}`);
      console.log(`  remote url: ${url}`);
      console.log(`  error: ${err}`);
      reject({resource: fileAddress, error: err});
    });
  });
}

/**
 * Creates an association between the cached file and the original FileAddress in the database
 * @param {custom object} downloadResult Data about the last download attempt's result. The actual content may vary depending on the situation.
 */
async function associateCachedFile (downloadResult) {

  const uri = downloadResult.resource.uri.value;
  const name = downloadResult.cachedFileName;
  const extension = downloadResult.extension;
  const date = Date.now();

  //--- get the file's size
  const stats = fs.statSync(downloadResult.cachedFileAddress);
  const fileSize = stats.size;

  //--- read data from HTTP response heades
  const headers = downloadResult.result.headers;
  const contentType = getContentTypeFrom(headers);

  try {
    //First create the virtual file.
    let fileObjectUri = FILE_RESOURCES_PATH + uuid(); //WE assume trailing slash
    let result = await createVirtualFileDataObject(fileObjectUri,
                                                  uri,
                                                  name,
                                                  contentType,
                                                  fileSize,
                                                  extension,
                                                  date);
    //create the physical file
    let physicalUri = 'share://' + downloadResult.cachedFileName; //we assume filename here
    let resultPhysicalFile = await createPhysicalFileDataObject(physicalUri,
                                                                fileObjectUri,
                                                                name,
                                                                contentType,
                                                                fileSize,
                                                                extension,
                                                                date);
  }
  catch (err) {
    console.log('Error while associating a downloaded file to a FileAddress object');
    console.log(err);
    console.log(`  downloaded file: ${downloadResult.cachedFileAddress}`);
    console.log(`  FileAddress object: ${uri}`);
    throw err;
  }
}

/**
 * Deletes a file.
 * Is intended to be used for deleting orphant files after a failure.
 * @param {string} path Local path to a file
 */
function cleanUpFile (path){
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
