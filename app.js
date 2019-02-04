import { app, uuid, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeInt, sparqlEscapeDateTime } from 'mu';
import { querySudo as  query, updateSudo as update } from './auth-sudo';
import fs from 'fs';
import url from 'url';
import needle from 'needle';
import { CronJob } from 'cron';
import request from 'request';

/** Schedule export cron job */
const cronFrequency = process.env.CRON_PATTERN || '0 0 20 * * *';

new CronJob(cronFrequency, function() {
  console.log(`download triggered by cron job at ${new Date().toISOString()}`);
  request.post('http://localhost/fetch-urls');
}, null, true);

needle.defaults({ follow: 3, user_agent: 'LBLOD scaper'});
app.post('/fetch-urls', async function( req, res, next ) {
  try {
    const r = await query(`
            PREFIX nie:     <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
            PREFIX nfo:     <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
            SELECT DISTINCT ?g ?url ?inzending ?fileAddress
            WHERE {
              GRAPH ?g {
                ?inzending <http://mu.semte.ch/vocabularies/ext/supervision/fileAddress> ?fileAddress;
                           <http://www.w3.org/ns/adms#status> <http://data.lblod.info/document-statuses/verstuurd>.
              }
              ?fileAddress a <http://mu.semte.ch/vocabularies/ext/FileAddress>; <http://mu.semte.ch/vocabularies/ext/fileAddress> ?url
              FILTER(NOT EXISTS {
                 ?resource a nfo:FileDataObject; nie:dataSource ?fileAddress.
              })
            }
    `);
    for (const binding of r.results.bindings) {
      try {
        const parsed_url = url.parse(binding.url.value);
        var contentType = "";
        needle.head(parsed_url.href, function(err, resp) {
          if (!err) {
            contentType = resp.headers['content-type'] ? resp.headers['content-type'] : '';
          }
        });
        const remoteFilename = parsed_url.pathname.split('/').pop();
        const remoteExtension = remoteFilename.includes(".") ? remoteFilename.split('.').pop() : '';
        const extension = remoteExtension === 'pdf' || contentType.includes("application/pdf") ? 'pdf' : 'html';
        const id = uuid();
        const filename = `${id}.${extension}`;
        const path = `/share/${filename}`;
        const file = fs.createWriteStream(path);
        const res = needle.get(parsed_url.href);
        res.pipe(file);
        res.on('done', async function(err) {
          file.end();
          if (err) {
            console.log(`An error ocurred for ${parsed_url.href}: ` + err.message);
            fs.unlinkSync(path);
          }
          else {
            const stats = fs.statSync(path);
            const fileSize = stats.size;
            const uploadUuid = uuid();
            const uploadResource = `http://mu.semte.ch/services/download-url-service/${uploadUuid}`;
            const fileResource = `share://${filename}`;
            const created = new Date();
            const graph = binding.g.value;
            const inzending = binding.inzending.value;
            const source = binding.fileAddress.value;
            const query = `PREFIX nie:     <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
                             PREFIX nfo:     <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
                             PREFIX dcterms: <http://purl.org/dc/terms/>
                             PREFIX dbo:     <http://dbpedia.org/ontology/>
                             PREFIX mu:      <http://mu.semte.ch/vocabularies/core/>
                             INSERT DATA {
                               GRAPH ${sparqlEscapeUri(graph)} {
                                   ${sparqlEscapeUri(inzending)} nie:hasPart ${sparqlEscapeUri(uploadResource)}.
                               }
                               GRAPH <http://mu.semte.ch/graphs/public> {
                                ${sparqlEscapeUri(uploadResource)} a nfo:FileDataObject;
                                                        nfo:fileName ${sparqlEscapeString(remoteFilename)};
                                                        mu:uuid ${sparqlEscapeString(uploadUuid)};
                                                        dcterms:format ${sparqlEscapeString(contentType)};
                                                        nfo:fileSize ${sparqlEscapeInt(fileSize)};
                                                        dbpedia:fileExtension ${sparqlEscapeString(remoteExtension)};
                                                        dcterms:created ${sparqlEscapeDateTime(created)};
                                                        dcterms:modified ${sparqlEscapeDateTime(created)};
                                                        nie:dataSource ${sparqlEscapeUri(source)}.
                               ${sparqlEscapeUri(fileResource)} a nfo:FileDataObject;
                                                                nie:dataSource ${sparqlEscapeUri(uploadResource)};
                                                                nfo:fileName ${sparqlEscapeString(`${filename}`)};
                                                                mu:uuid ${sparqlEscapeString(id)};
                                                                dcterms:format ${sparqlEscapeString(contentType)};
                                                                nfo:fileSize ${sparqlEscapeInt(fileSize)};
                                                                dbpedia:fileExtension ${sparqlEscapeString(extension)};
                                                                dcterms:created ${sparqlEscapeDateTime(created)};
                                                                dcterms:modified ${sparqlEscapeDateTime(created)}.
                               }}`;
            await update(query);
          }
        });
      }
      catch(e) {
        console.log(`downloading ${binding.url.value} resulted in an error`);
        console.log(e);
      }
    }
    res.sendStatus(202);
  }
  catch(e) {
    next(new Error(e));
    res.sendStatus(500);
  }
});
