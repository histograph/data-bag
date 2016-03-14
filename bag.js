'use strict';

var fs          = require('fs');
var path        = require('path');
var Promise     = require('bluebird');
var request     = require('request');
var progress    = require('request-progress');
var yauzl       = require('yauzl');
var mkdirp      = require('mkdirp');
var highland    = require('highland');

var sax         = require('sax');
var saxpath     = require('saxpath');
var xml2js      = require('xml2js');

var workerFarm      = require('worker-farm');
const NUM_CPUS      = require('os').cpus().length;
const FARM_OPTIONS  = {
  maxConcurrentWorkers: require('os').cpus().length,
  maxCallsPerWorker: Infinity,
  maxConcurrentCallsPerWorker: 1
};

var buildingsworkers = workerFarm(
  FARM_OPTIONS,
  require.resolve('./helpers/buildingsextractor.js'),
  ['extractFromFile']
);

var addressworkers = workerFarm(
  FARM_OPTIONS,
  require.resolve('./helpers/addressesextractor.js'),
  ['extractFromFile']
);

var publicSpacesWorkers = workerFarm(
  FARM_OPTIONS,
  require.resolve('./helpers/publicspacesextractor.js'),
  ['extractFromFile']
);

var config = require('../config/index.js');

module.exports = {
  download: download,
  extractDownloadSize: extractDownloadSize,
  downloadDataFile: downloadDataFile,
  unzip: unzip,
  extractZipfile: extractZipfile,
  convert: convert,
  mapFilesToJobs: mapFilesToJobs,
  mkdir: mkdir,
  steps: [
    download,
    unzip,
    convert
  ]
};

function download(config, dir, writer, callback) {
  console.log(`Downloading...`);
  return extractDownloadSize(config.feedURL)
    .then(size => downloadDataFile(config.baseDownloadUrl, config.datafilename, dir, size))
    .then((fullPath) => {
      console.log(`${new Date()} download of ${fullPath} complete!`);
      return callback;
    })
    .catch(error => {
      console.error(`${new Date()} Download failed due to ${error}`);
      return callback(error);
    });
}

function extractDownloadSize(atomURL) {
  return new Promise((resolve, reject) => {
    request(atomURL,
      (err, response, body) => {
        if (err) return reject(err);
        if (!response) return reject(new Error(`No response returned from request to ${atomURL}`));
        if (response.statusCode != 200) {
          return reject(new Error(`Unexpected request to ${atomURL} response status ${response.statusCode}`));
        }
        if (!body) return reject(new Error(`The request to ${atomURL} did not return a response body`));

        var parser = new xml2js.Parser();
        parser.parseString(body, (err, result) => {
          if (err) return reject(new Error(`Error parsing body ${body} \n ${err.stack}`));
          console.log(`Length: ${JSON.stringify(result.feed.entry[0].link[0].$.length, null, 2)}`);
          resolve(parseInt(result.feed.entry[0].link[0].$.length));
        });
      }
    );
  });
}

function downloadDataFile(baseURL, filename, dir, size) {
  return new Promise((resolve, reject) => {
    var fullZipFilePath = path.join(dir, filename);
    console.log(`Getting ${baseURL + filename}:`);
    console.log(`Total size: ${size}`);

    progress(request
      .get(baseURL + filename), {
        throttle: 2000,
        delay: 1000
      })
      .on('progress', state => {
        console.log(`Download progress: ${((state.size.transferred / size) * 100).toFixed(0)}%`);
      })
      .on('error', err => reject(err))
      .on('end', () => {
        console.log(`Download progress: 100%`);
        resolve(fullZipFilePath);
      });
  });
}

function unzip(config, dir, writer, callback) {
  console.log(`WARNING, make sure you have at least 45 Gb of free disk space for extraction, or press Ctrl-c to abort.`);
  console.log(`The unzip phase itself can take up to an hour and will extract about 4.000 XML files.`);
  console.log(`Since the zipfile consists of sub-zipfiles of unknown size, there cannot be given an estimation of remaining time.`);
  console.log(`The process will appear to be frozen for quite some time, especially on the ***PND***.zip file.`);
  console.log(`However, this will at least spare you the logging of about 4000 file names.`);
  return extractZipfile(path.join(dir, config.datafilename), dir)
    .then(() => {
      console.log(`${new Date()} extraction complete!`);
      return callback;
    })
    .catch(error => {
      console.error(`${new Date()} Extraction failed due to ${error}`);
      return callback(error);
    });
}

function extractZipfile(zipfilename, extractdir) {
  return new Promise((resolve, reject) => {
    console.log('extractdir: ', extractdir, '\n');
    mkdirp(extractdir);

    console.log('zipfilename: ', zipfilename, '\n');
    yauzl.open(zipfilename, { lazyEntries: true }, (err, zipfile) => {
      if (err) reject(err);

      zipfile.readEntry();

      zipfile.on('entry', entry => {
        if (/\/$/.test(entry.fileName)) {
          // directory file names end with '/'
          mkdirp(entry.fileName,
            err => {
              if (err) throw err;
              return zipfile.readEntry();
            });

        }

        // file entry
        zipfile.openReadStream(entry, (err, readStream) => {
          if (err) {
            console.log(`Error reading ${entry.fileName}`);
            reject(err);
          }

          // ensure parent directory exists
          mkdirp(path.dirname(entry.fileName), err => {
            if (err) reject(err);

            readStream.pipe(fs.createWriteStream(path.join(extractdir, entry.fileName)));

            readStream.on('end', () => {
              if (entry.fileName.slice(-4) === '.zip') {
                extractZipfile(path.join(extractdir, entry.fileName), extractdir)
                  .then(() => {
                    console.log(`Extracted subzip ${entry.fileName}`);
                    zipfile.readEntry();
                  });
              } else {
                zipfile.readEntry();
              }
            });

            readStream.on('error', err => reject(err));

          });
        });

      });

      zipfile.on('end', () => resolve());
    });
  });
}

function convert(config, dir, writer, callback) {
  var extractDir = path.join(config.data.generatedDataDir, 'data-bag');
  console.log(`WARNING, make sure you have at least 45 Gb of free disk space for conversion, or press Ctrl-c to abort.`);
  var jobs = mapFilesToJobs(dir, extractDir);

  mkdir(extractDir)
    .then(() => {
      var jobStream = highland(jobs);

      jobStream
        .map(job => {
          console.log(`Processing ${job.inputFile} to output to ${job.outputPITsFile} and ${job.outputRelationsFile}`);
          return highland.wrapCallback(job.converter.extractFromFile(job.inputFile, job.outputPITsFile, job.outputRelationsFile));
        })
        .parallel(NUM_CPUS - 1)
        .errors(err => {
          fs.appendFileSync(path.join(__dirname, 'error.log'), JSON.stringify(err));
          return console.log(`Stream threw error. Wrote error to error.log.`);
        })
        .toArray(result => {
          console.log(`Done processing all files!`);
          return callback(null, result)
        });

    })
    .catch(err => callback(err, null));
}

function mkdir(path) {
  return new Promise((resolve, reject) => {
    mkdirp(path, err => {
      if (err) {
        console.log(`Error during directory creation: ${err}`);
        reject(err);
      }
      resolve()
    });
  });
}

function mapFilesToJobs(dir, extractDir) {
  var fileTypes = {
    PND: {
      converter: buildingsworkers,
      outputPITsFile: 'pand.pits.ndjson',
      outputRelationsFile: 'pand.relations.ndjson'
    },
    NUM: {
      converter: addressworkers,
      outputPITsFile: 'adres.pits.ndjson',
      outputRelationsFile: 'adres.relations.ndjson'
    },
    OPR: {
      converter: publicSpacesWorkers,
      outputPITsFile: 'openbareruimte.pits.ndjson',
      outputRelationsFile: 'openbareruimte.relations.ndjson'
    }
  };

  return fs.readdirSync(dir)
    .filter(file => file.slice(-4) === '.xml')
    .map(file => {
      var type = file.slice(4, 7);
      var job = {};
      if (!fileTypes[type]) return null;
      job.converter = fileTypes[type].converter;
      job.inputFile = path.resolve(path.join(dir, file));
      job.outputPITsFile = path.resolve(path.join(extractDir, fileTypes[type].outputPITsFile));
      job.outputRelationsFile = path.resolve(path.join(extractDir, fileTypes[type].outputRelationsFile));
      return job;
    })
    .filter(job => (job));
}
