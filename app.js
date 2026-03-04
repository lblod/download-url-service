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
import RootCas from 'ssl-root-cas/latest.js';
import https from 'https';
import bodyParser from 'body-parser';
import { ClientCredentials } from 'simple-oauth2';
import FileType from 'file-type';
import { isText } from 'istextorbinary';
import * as htmlparser2 from 'htmlparser2'
import contentDisposition from 'content-disposition';
import url from 'node:url';

const CACHING_MAX_RETRIES = parseInt(process.env.CACHING_MAX_RETRIES || 30);
const FILE_STORAGE = process.env.FILE_STORAGE || '/share';
const REMOVE_AUTHENTICATION_SECRETS_AFTER_DOWLOAD = (process.env.REMOVE_AUTHENTICATION_SECRETS_AFTER_DOWLOAD || 'true') == 'true';
const DEFAULT_TEXT_FORMAT = process.env.DEFAULT_TEXT_FORMAT || '.txt';

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
    res.status(404).end(`Something went wrong while retrieving remote data object with id ${uuid}`);
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

  // Downloading the file as a temporary file
  let tmpDownloadResult = await downloadFile(remoteObject, requestHeaders, credentialsType, '.tmp');
  // Update its type, extention, path by its content type or by guessing it
  let downloadResult = await updateFileType(tmpDownloadResult);
  // Store the final file in the store
  let physicalFileUri = await associateCachedFile(downloadResult, remoteObject);

  //TODO: this needs re-thinking
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
      if(REMOVE_AUTHENTICATION_SECRETS_AFTER_DOWLOAD){
        await deleteCredentials(o);
      }
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
  let url = '';
  try {
    url = remoteObject.url.value;

    const requestObject = {url};
    headers = headers || {};
    requestObject.options = { headers };

    if(credentialsType) {
      await appendAuthenticationHeaders(requestObject, headers, remoteObject, credentialsType);
    }

    let response = await fetch(requestObject.url, requestObject.options);

    await saveHttpStatusCode(remoteObject.subject.value, response.status);

    if (response.ok) { // res.status >= 200 && res.status < 300
      //--- Status: OK
      //--- create file attributes
      const extension = '.tmp';
      const uuidName = uuid();

      let fileName;
      const fileNameExt = tryGetFilename(response, remoteObject);
      let extensionFromFileName = path.extname(fileNameExt || '');
      if (extensionFromFileName !== ''
          && extensionFromFileName !== '.'
          && mime.lookup(extensionFromFileName)) {
        // If useful extension, keep it
        fileName = path.basename(fileNameExt, extensionFromFileName);
      } else {
        // Not useful extension, keep as part of filename
        // Also when fileNameExt is undefined
        fileName = fileNameExt || uuidName;
        extensionFromFileName = undefined;
      }
      
      const physicalFileName = [uuidName, extension].join('');
      const logicalFileName = [fileName, extension].join('');
      const physicalPath = path.join(FILE_STORAGE, physicalFileName);

      //--- write the file
      try {
        await saveStreamToDisk(response.body, physicalPath);
        return {
          remoteObject,
          response,
          physicalPath,
          physicalFileName,
          logicalFileName,
          extension,
          extensionFromFileName,
        };
      } catch (err) {
        //--- We need to clean up on error during file writing
        console.log(`${physicalPath} failed writing to disk, cleaning up...`);
        cleanUpFile(physicalPath);
        throw err;
      }
    } else {
      //--- NO OK
      throw Error(`Response code http ${response.status}`);
    }
  } catch (err) {
    console.error('Error while downloading a remote resource:');
    console.error(`Remote resource: ${remoteObject.subject.value}`);
    console.error(`Remote url: ${url || '[ Not attached to remoteDataObject ]'}`);
    console.error(`Error: ${err}`);
    await saveCacheError(remoteObject.subject.value, err);
    throw err;
  }
}

/**
 * Creates an association between the cached file and the original FileAddress in the database
 */
async function associateCachedFile(downloadResult, remoteDataObjectQueryResult) {

  const uri = downloadResult.remoteObject.subject.value;
  const name = downloadResult.logicalFileName;
  const extension = downloadResult.extension;
  const date = Date.now();

  //--- get the file's size
  const stats = fs.statSync(downloadResult.physicalPath);
  const fileSize = stats.size;

  //--- read data from the extension
  const contentType = getContentTypeFromExtension(extension);

  try {
    //create the physical file
    let physicalUri = 'share://' + downloadResult.physicalFileName;
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
    console.error(`  downloaded file: ${downloadResult.physicalPath}`);
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
  return mime.lookup(extension);
}

/**
 * Parses response headers to get the file extension
 *
 * @param {array} headers HTML response header
 */
function getExtensionFrom(headers) {
  const contentType = headers.get('content-type');
  return `.${mime.extension(contentType)}`;
}

/**
 * Try to get the full filename from the HTTP response, with extension
 * included. This tries multiple mechanisms in order: get the filename from the
 * HTTP header 'Content-Disposition', get the filename from URL parameters
 * (guesswork), get a suggested filename from the remote data object, or get
 * the filename from the path in the URL.
 *
 * @function
 * @param {HTTP Response} response - Response object from the HTTP request.
 * @param {Object} remoteObject - Remote data object query result.
 * @returns {String | undefined} Filename with extension, or last part of the
 * path in the URL. Returns undefined when URL path is undefined.
 */
function tryGetFilename(response, remoteObject) {
  const headerCD = response.headers.get('Content-Disposition');
  if (headerCD) {
    const disposition = contentDisposition.parse(headerCD);
    if (disposition && disposition.type === 'attachment') {
      const parameters = disposition.parameters;
      if (parameters.filename)
        return parameters.filename;
    }
  }

  // Look for URL parameters that might give a filename
  const url = new URL(response.url);
  const parameters = url.searchParams;
  const filename1 = parameters.get('filename');
  if (filename1) return filename1;
  const filename2 = parameters.get('file');
  if (filename2) return filename2;
  const filename3 = parameters.get('name');
  if (filename3) return filename3;

  // Try the suggested filename from the remote data object (if available)
  if (remoteObject.suggestedFilename?.value) {
    return remoteObject.suggestedFilename.value;
  }

  // Last resort: return last segment of URL path
  const urlPath = url.pathname || '';
  const pathSegments = urlPath.split('/');
  const lastSegment = pathSegments.pop();
  return lastSegment;
}

/**
 * Save stream to the path on disk.
 *
 * @async
 * @function
 * @param {Stream} stream - Stream to pipe into a file on disk
 * @param {String} path - Location to save the file
 * @returns {undefined} Nothing
 */
async function saveStreamToDisk(stream, path) {
  return new Promise((resolve, reject) => {
    const writeStream = fs.createWriteStream(path);
    stream.pipe(writeStream);
    writeStream.on('close', () => resolve());
    writeStream.on('error', reject);
  });
}

/**
 * Updates the extension of a file. It first trusts the extension that was
 * given to the file. If that does not exist, it looks at the Content-Type HTTP
 * header to deduce an extension, or if the file is a raw data stream, guess it
 * by its content.
 *
 * @async
 * @function
 * @param {Object} downloadResult - Result of the download
 * @returns {Object} Same downloadResult object that was given is parameter,
 * but with updated values.
 */
async function updateFileType(downloadResult) {
  if (downloadResult.extensionFromFileName) {
    const updatedResult = await updatePhysicalFileExtension(
      downloadResult.physicalPath,
      downloadResult.extensionFromFileName,
    );
    downloadResult.physicalPath = updatedResult.physicalPath;
    downloadResult.physicalFileName = updatedResult.physicalFileName;
    downloadResult.logicalFileName = updateLogicalFileExtension(
      downloadResult.logicalFileName,
      downloadResult.extensionFromFileName,
    );
    downloadResult.extension = downloadResult.extensionFromFileName;
    return downloadResult;
  }

  const contentType = downloadResult.response.headers.get('content-type');
  const extension = mime.extension(contentType);

  if (contentType === 'application/octet-stream' || !extension) {
    // If content type in binary or if we didn't find an extension yet, try guessing
    const guessedExtension = await guessRealExtension(downloadResult.physicalPath);

    if (guessedExtension && (guessedExtension != downloadResult.extension)) {
      const updatedResult = await updatePhysicalFileExtension(
        downloadResult.physicalPath,
        guessedExtension
      );
      downloadResult.physicalPath = updatedResult.physicalPath;
      downloadResult.physicalFileName = updatedResult.physicalFileName;
      downloadResult.logicalFileName = updateLogicalFileExtension(
        downloadResult.logicalFileName,
        guessedExtension,
      );
      downloadResult.extension = guessedExtension;
      return downloadResult;
    }
  }

  if (extension) {
    // Weird binary case discarded, we can trust the content-type and deduce the extension from it
    const formattedExtension = `.${extension}`;
    const updatedResult = await updatePhysicalFileExtension(
      downloadResult.physicalPath,
      formattedExtension
    );
    downloadResult.physicalPath = updatedResult.physicalPath;
    downloadResult.physicalFileName = updatedResult.physicalFileName;
    downloadResult.logicalFileName = updateLogicalFileExtension(
      downloadResult.logicalFileName,
      formattedExtension,
    );
    downloadResult.extension = formattedExtension;
  }

  return downloadResult;
}

/**
 * Try deducing file extension using magic numbers and parsing
 *
 * @param fileAddress Location of the saved file
 */
async function guessRealExtension(fileAddress) {
  const fileType = await FileType.fromFile(fileAddress);
  if (fileType) {
    // File type can be deduced from magic numbers
    return `.${fileType.ext}`;
  } else {
    const bufferedFile = fs.readFileSync(fileAddress, 'utf8');
    const isTextFile = isText(null, bufferedFile);

    if (isTextFile) {
      // File is a text file, guessing if it's html, else default text format
      const doctype = getHtmlDoctypeFromBuffer(bufferedFile);
      if (doctype) {
        return '.html';
      } else {
        return DEFAULT_TEXT_FORMAT;
      }
    }
  }

  // Default to .bin
  return '.bin';
}

/**
 * Checks if a document has an html doctype
 *
 * @param {Buffer} bufferedFile The buffered file to parse
 */
function getHtmlDoctypeFromBuffer(bufferedFile) {
  // Checking if we can find a closing tag. If yes, assuming HTML
  // Can bring false positives (XML files for example would become HTML)
  try {
    let hasClosingTag = false;
    const parser = new htmlparser2.Parser({
        onclosetag(tagname) {
          hasClosingTag = true;
        }
    });

    parser.write(bufferedFile);
    parser.end();

    return hasClosingTag;
  } catch (err) {
    console.error('An error occured while trying to parse html');
    console.error(err);
    return false;
  }
}

/**
 * Rename a file by changing its extension.
 *
 * @async
 * @function
 * @param {String} physicalPath - Path of the saved file that needs a new
 * extension.
 * @param {String} extension - The new extension to save the file with, e.g.
 * ".pdf".
 * @returns {Object} Object with `physicalPath` and `physicalFileName`
 * containing the updated values.
 */
async function updatePhysicalFileExtension(physicalPath, extension) {
  const basename = path.basename(physicalPath, path.extname(physicalPath));
  const fileName = basename + extension;
  const newPhysicalPath = path.join(path.dirname(physicalPath), fileName);
  await fs.move(physicalPath, newPhysicalPath);
  return {
    physicalPath: newPhysicalPath,
    physicalFileName: fileName
  };
}

/**
 * Change the extension of a filename to something new.
 *
 * @function
 * @param {String} logicalFileName - Filename to change the extension of.
 * @param {String} extension - New extension.
 * @returns {String} Same filename, but with the extension changed.
 */
function updateLogicalFileExtension(logicalFileName, extension) {
  const basename = path.basename(logicalFileName, path.extname(logicalFileName));
  const fileName = basename + extension;
  return path.join(path.dirname(logicalFileName), fileName);
}

/*
 * Adds authentication headers to the requestObject.
 * Note: has side effects
 */
async function appendAuthenticationHeaders(requestObject, headers, remoteObject, credentialsType) {
  if (credentialsType == BASIC_AUTH) {
    const credentialsInfo = await getBasicCredentialsForRemoteDataObject(remoteObject.subject);
    const encodedCredentials = Buffer.from(`${credentialsInfo.user.value}:${credentialsInfo.pass.value}`).
        toString('base64');
    requestObject.options.headers.Authorization = `Basic ${encodedCredentials}`;
  }
  else if (credentialsType == OAUTH2) {
    const credentialsInfo = await getOauthCredentialsForRemoteDataObject(
      remoteObject.subject
    );

    const tokenURL = new URL(credentialsInfo.accessTokenUri.value);

    const config = {
      client: {
        id: credentialsInfo.clientId.value,
        secret: credentialsInfo.clientSecret.value,
      },
      auth: {
        tokenHost: `${tokenURL.protocol}//${tokenURL.host}`,
        tokenPath: tokenURL.pathname,
      }
    };

    const client = new ClientCredentials(config);
    const tokenResponse = await client.getToken({
      scope: credentialsInfo.scope?.value,
    });
    

    requestObject.options = {
      method: "GET",
      headers: {
        ...headers,
        Authorization: `Bearer ${tokenResponse.token.access_token}`,
      },
      url: requestObject.url,
    };
  }

  return requestObject;
}
