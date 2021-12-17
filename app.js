import { app, errorHandler, uuid } from 'mu';
import { waitForDatabase } from './database-utils';
import {
  getRemoteDataObjectByStatus,
  getRequestHeadersForRemoteDataObject,
  getCredentialsTypeForRemoteDataObject,
  getBasicCredentialsForRemoteDataObject,
  getOauthCredentialsForRemoteDataObject,
  updateStatus,
  createDownloadEvent,
  getDownloadEvent,
  updateDownloadEvent,
  createPhysicalFileDataObject,
  updateDownloadEventOnSuccess,
  saveHttpStatusCode,
  saveCacheError,
  getRemoteDataObject,
  deleteCredentials,
  READY,
  ONGOING,
  SUCCESS,
  FAILURE,
  BASIC_AUTH,
  OAUTH2
} from './queries';
import flatten from 'lodash.flatten';
import uniq from 'lodash.uniq';
import fetch from 'node-fetch';
import fs from 'fs-extra';
import mime from 'mime-types';
import path from 'path';
import RootCas from 'ssl-root-cas/latest';
import https from 'https';
import bodyParser from 'body-parser';
import ClientOAuth2 from 'client-oauth2';
import FileType from 'file-type';

const CACHING_MAX_RETRIES = parseInt(process.env.CACHING_MAX_RETRIES || 30);
const FILE_STORAGE = process.env.FILE_STORAGE || '/share';
const DEFAULT_EXTENSION = '.html';
const DEFAULT_CONTENT_TYPE = 'text/html';
const REMOVE_AUTHENTICATION_SECRETS_AFTER_DOWLOAD = (process.env.REMOVE_AUTHENTICATION_SECRETS_AFTER_DOWLOAD || 'true') == 'true';
const GUESS_FILE_TYPE_BINARY_FILES = (process.env.GUESS_FILE_TYPE_BINARY_FILES || 'true') == 'true';

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

app.use(bodyParser.json({
  type: function(req) {
    return /^application\/json/.test(req.get('content-type'));
  }
}));

app.get('/', function(req, res) {
  res.send(`Welcome to the dowload url service.`);
});

app.post('/process-remote-data-objects', async function(req, res) {
  const delta = req.body;
  const remoteDataObjectUris = getRemoteDataObjectsFromDelta(delta);
  console.log(`Found ${remoteDataObjectUris.length} new remote data objects in the delta message`);
  processDownloads(remoteDataObjectUris);
  res.send({message: `Started.`});
});

// Endpoint to ease development
app.post('/process-remote-data-object/:uuid', async function(req, res, next) {
  const uuid = req.params.uuid;
  try {
    const remoteDataObjectUri = await getRemoteDataObject(uuid);
    console.log(`Found new remote data object ${remoteDataObjectUri} with uuid ${uuid}`);
    processDownloads([remoteDataObjectUri]);
    res.send({message: `Started.`});
  } catch (e) {
    console.log(`Something went wrong while retrieving remote data object with id ${uuid}`);
    console.log(e);
  }
});

app.use(errorHandler);

function getRemoteDataObjectsFromDelta(delta) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  const remoteDataObjectUris = inserts.filter(triple => {
    return triple.predicate.type == 'uri'
        && triple.predicate.value == 'http://www.w3.org/ns/adms#status'
        && triple.object.type == 'uri'
        && triple.object.value == 'http://lblod.data.gift/file-download-statuses/ready-to-be-cached';
  }).map(triple => triple.subject.value);
  return uniq(remoteDataObjectUris);
}

async function processDownloads(remoteDataObjectUris) {
  const remoteObjects = await getRemoteDataObjectByStatus(READY, remoteDataObjectUris);

  //create associated download events and lock in DB.
  for (let o of remoteObjects) {
    let dlEventUri = await createDownloadEvent(o.subject.value);
    await updateStatus(o.subject.value, ONGOING);
    o.dlEventUri = dlEventUri;
  }

  for (let o of remoteObjects) {
    try {
      await performDownloadTask(o, o.dlEventUri);
    } catch (error) {
      handleDownloadTaskError(error, o, o.dlEventUri);
    }
  }
}

async function performDownloadTask(remoteObject, downloadEventUri) {
  let requestHeaders = await getRequestHeadersForRemoteDataObject(remoteObject.subject);
  requestHeaders = requestHeaders.reduce((acc, h) => {
    acc[`${h.headerName.value}`] = h.headerValue.value;
    return acc;
  }, {});

  const credentialsType = await getCredentialsTypeForRemoteDataObject(remoteObject.subject);

  let downloadResult = await downloadFile(remoteObject, requestHeaders, credentialsType);

  let physicalFileUri = await associateCachedFile(downloadResult, remoteObject);

  if(REMOVE_AUTHENTICATION_SECRETS_AFTER_DOWLOAD){
    await deleteCredentials(remoteObject, credentialsType);
  }

  await updateDownloadEventOnSuccess(downloadEventUri, physicalFileUri);

  await updateStatus(remoteObject.subject.value, SUCCESS);

  console.log(`Processed ${remoteObject.subject.value}, with url: ${remoteObject.url.value}`);
}

async function handleDownloadTaskError(error, remoteObject, downloadEventUri) {
  console.error(`Error for ${remoteObject.subject.value} and task ${downloadEventUri}`);
  console.error(error.message);
  await updateStatus(downloadEventUri, FAILURE);
  scheduleRetryProcessing(remoteObject, downloadEventUri);
}

async function scheduleRetryProcessing(remoteObject, downloadEventUri) {
  let downloadEvent = await getDownloadEvent(downloadEventUri);
  console.log(
      `Download event ${downloadEventUri} retried ${downloadEvent.numberOfRetries.value}/${CACHING_MAX_RETRIES} already`);
  if (downloadEvent.numberOfRetries.value >= CACHING_MAX_RETRIES) {
    await updateStatus(remoteObject.subject.value, FAILURE);
    await updateStatus(downloadEventUri, FAILURE);
    console.log(`Stopping retries for ${remoteObject.subject.value} and task ${downloadEventUri})`);

    if(REMOVE_AUTHENTICATION_SECRETS_AFTER_DOWLOAD){
      await deleteCredentials(remoteObject);
    }
    return;
  }

  let waitTime = calcTimeout(parseInt(downloadEvent.numberOfRetries.value));
  console.log(`Expecting next retry for ${remoteObject.subject.value} and task ${downloadEventUri} in about ${waitTime /
  1000} seconds`);
  setTimeout(async () => {
    try {
      console.log(`Retry for ${remoteObject.subject.value} and task ${downloadEventUri}`);
      await updateDownloadEvent(downloadEventUri, parseInt(downloadEvent.numberOfRetries.value) + 1, ONGOING);
      await performDownloadTask(remoteObject, downloadEventUri);
    } catch (error) {
      handleDownloadTaskError(error, remoteObject, downloadEventUri);
    }
  }, waitTime);

}

async function rescheduleTasksOnStart() {
  let remoteObjects = await getRemoteDataObjectByStatus(ONGOING);
  for (let o of remoteObjects) {
    try {
      await scheduleRetryProcessing(o, o.downloadEventUri.value);
    } catch (error) {
      //if rescheduling fails, we consider there is something really broken...
      console.log(`Fatal error for ${o.subject.value}`);
      await updateStatus(o.subject.value, FAILURE);
    }
  }
}

function calcTimeout(x) {
  //expected to be milliseconds
  return Math.round(Math.exp(0.3 * x + 10)); //I dunno I just gave it a shot
}

/**
 * Downloads the resource and takes care of errors.
 * Throws exception on failed download.
 */
async function downloadFile(remoteObject, headers, credentialsType) {
  const url = remoteObject.url.value;

  const requestBody = {url};
  if (Object.keys(headers).length > 0) {
    requestBody.options = {headers};
  }

  if (credentialsType == BASIC_AUTH) {
    const credentialsInfo = await getBasicCredentialsForRemoteDataObject(remoteObject.subject);
    const encodedCredentials = Buffer.from(`${credentialsInfo.user.value}:${credentialsInfo.pass.value}`).
        toString('base64');
    requestBody.options.headers.Authorization = `Basic ${encodedCredentials}`;
  }

  if (credentialsType == OAUTH2) {
    const credentialsInfo = await getOauthCredentialsForRemoteDataObject(remoteObject.subject);

    const body = {
      'client_id': credentialsInfo.clientId.value,
      'client_secret': credentialsInfo.clientSecret.value
    };
    if (credentialsInfo.resource && credentialsInfo.resource.value)
      body['resource'] = credentialsInfo.resource.value;

    const oauthClient = new ClientOAuth2({
      clientId: credentialsInfo.clientId.value,
      clientSecret: credentialsInfo.clientSecret.value,
      accessTokenUri: credentialsInfo.accessTokenUri.value,
      authorizationGrants: ['credentials'],
      body: body
    });
    const tokenResponse = await oauthClient.credentials.getToken();

    requestBody.options = tokenResponse.sign({
      method: 'GET',
      url: requestBody.url
    });
  }

  try {
    let response = await fetch(requestBody.url, requestBody.options);
    await saveHttpStatusCode(remoteObject.subject.value, response.status);
    if (response.ok) { // res.status >= 200 && res.status < 300
      //--- Status: OK
      //--- create file attributes
      let extension = getExtensionFrom(response.headers);
      let bareName = uuid();
      let physicalFileName = [bareName, extension].join('');
      let localAddress = path.join(FILE_STORAGE, physicalFileName);

      //--- write the file
      try {
        await saveFileToDisk(response, localAddress);

        const contentType = response.headers.get('content-type');

        if (GUESS_FILE_TYPE_BINARY_FILES && contentType == 'application/octet-stream') {
          const newExtension = await getRealExtension(localAddress);
          if (newExtension) {
            const updateResult = await updateFileExtension(localAddress, newExtension);
            localAddress = updateResult.localAddress;
            physicalFileName = updateResult.physicalFileName;
            extension = newExtension;
          }
        }

        return {
          resource: remoteObject,
          result: response,
          cachedFileAddress: localAddress,
          cachedFileName: physicalFileName,
          bareName: bareName,
          extension: extension
        };
      } catch (err) {
        //--- We need to clean up on error during file writing
        console.log(`${localAddress} failed writing to disk, cleaning up...`);
        cleanUpFile(localAddress);
        throw err;
      }
    } else {
      //--- NO OK
      throw Error(`Response code http ${response.status}`);
    }
  } catch (err) {
    console.error('Error while downloading a remote resource:');
    console.error(`  remote resource: ${remoteObject.subject.value}`);
    console.error(`  remote url: ${url}`);
    console.error(`  error: ${err}`);
    await saveCacheError(remoteObject.subject.value, err);
    throw err;
  }
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

  //--- read data from the extension
  const contentType = getContentTypeFromExtension(extension);

  try {
    //create the physical file
    let physicalUri = 'share://' + downloadResult.cachedFileName; //we assume filename here
    let resultPhysicalFile = await createPhysicalFileDataObject(
        physicalUri,
        remoteDataObjectQueryResult.subject.value,
        name,
        contentType,
        fileSize,
        extension,
        date);
    return physicalUri;
  } catch (err) {
    console.error('Error while associating a downloaded file to a FileAddress object');
    console.error(err);
    console.error(`  downloaded file: ${downloadResult.cachedFileAddress}`);
    console.error(`  FileAddress object: ${uri}`);
    await saveCacheError(uri, err);
    throw err;
  }
}

/**
 * Deletes a file.
 * Is intended to be used for deleting orphant files after a failure.
 * @param {string} path Local path to a file
 */
function cleanUpFile(path) {
  if (fs.existsSync(path)) {
    fs.unlinkSync(path);
  }
}

/**
 * Parses extension to get the file content-type
 *
 * @param {string} extension The extension of the file
 */
function getContentTypeFromExtension(extension) {
  return mime.lookup(extension) || DEFAULT_CONTENT_TYPE;
}

/**
 * Parses response headers to get the file extension
 *
 * @param {array} headers HTML response header
 */
function getExtensionFrom(headers) {
  const contentType = headers.get('content-type');
  return `.${mime.extension(contentType)}` || DEFAULT_EXTENSION;
}

/**
 * Save file, async way
 *
 * @param res Response of the fetch to download the file
 * @param address Location to save the file
 */
async function saveFileToDisk(res, address) {
  return new Promise((resolve, reject) => {
    const writeStream = fs.createWriteStream(address);
    res.body.pipe(writeStream);
    writeStream.on('close', () => resolve());
    writeStream.on('error', reject);
  });
}

/**
 * Try deducing file extension using maging numbers and parsing
 *
 * @param localAddress Location of the saved file
 */
async function getRealExtension(localAddress) {
  const fileType = await FileType.fromFile(localAddress)
  if (fileType) {
    // File type can be deduced from magic numbers
    return `.${fileType.ext}`;
  } else {
    // File could be a text file, checking for HTML
    const doctype = getHtmlDoctypeFromFile(localAddress);
    if (doctype) {
      return `.html`;
    }
  }
  return null;
}

/**
 * Checks if a document has an html doctype
 *
 * @param document The document to parse
 */
function getHtmlDoctypeFromFile(localAddress) {
  // In HTML, the doctype is mandatory. Other tags such as <html>, <head>, ... can
  // be omitted in certain circomstances, making it hard to use to determine if a file's
  // content is HTML. See also https://html.spec.whatwg.org/dev/syntax.html#syntax
  const htmlparser2 = require("htmlparser2");
  const content = fs.readFileSync(localAddress, 'utf8');
  const document = htmlparser2.parseDOM(content);

  if (document) {
    return document.find(element => {
      const isDoctype = element.name ? element.name.toLowerCase() == '!doctype' : false;
      const isHtml = element.nodeValue ? element.nodeValue.includes('html') : false;
      return isDoctype && isHtml;
    });
  }
  return null;
}

/**
 * Rename file by changing its extension, async way
 *
 * @param address Location of the saved file
 * @param extension The new extension to save the file with
 */
async function updateFileExtension(localAddress, extension) {
  const basename = path.basename(localAddress, path.extname(localAddress));
  const physicalFileName = basename + extension;
  const newLocalAddress = path.join(path.dirname(localAddress), physicalFileName);
  await fs.move(localAddress, newLocalAddress);
  return {
    localAddress: newLocalAddress,
    physicalFileName: physicalFileName
  };
}
