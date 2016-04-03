'use strict';

const fs = require('fs');
const path = require('path');
const Promise = require('bluebird');
const request = require('request');
const progress = require('request-progress');
const yauzl = require('yauzl');
const mkdirp = require('mkdirp');
const highland = require('highland');
const sax = require('sax');
const saxpath = require('saxpath');
const xml2js = require('xml2js');

const workerFarm = require('worker-farm');
const NUM_CPUS = require('os').cpus().length;
const FARM_OPTIONS = {
  maxConcurrentWorkers: require('os').cpus().length,
  maxCallsPerWorker: Infinity,
  maxConcurrentCallsPerWorker: 1
};

const buildingsworkers = workerFarm(
  FARM_OPTIONS,
  require.resolve('./helpers/buildingsextractor.js'),
  ['extractFromFile']
);

const addressworkers = workerFarm(
  FARM_OPTIONS,
  require.resolve('./helpers/addressesextractor.js'),
  ['extractFromFile']
);

const publicSpacesWorkers = workerFarm(
  FARM_OPTIONS,
  require.resolve('./helpers/publicspacesextractor.js'),
  ['extractFromFile']
);

const placesWorkers = workerFarm(
  FARM_OPTIONS,
  require.resolve('./helpers/placesextractor.js'),
  ['extractFromFile']
);

function extractDownloadSize(atomURL) {
  return new Promise((resolve, reject) => {
    request(atomURL,
      (err, response, body) => {
        if (err) return reject(err);
        if (!response) return reject(new Error(`No response returned from request to ${atomURL}`));
        if (response.statusCode !== 200) {
          return reject(new Error(`Unexpected request to ${atomURL} response status ${response.statusCode}`));
        }
        if (!body) return reject(new Error(`The request to ${atomURL} did not return a response body`));

        const parser = new xml2js.Parser();
        return parser.parseString(body, (error, result) => {
          if (error) return reject(new Error(`Error parsing body ${body} \n ${error.stack}`));
          console.log(`Length: ${JSON.stringify(result.feed.entry[0].link[0].$.length, null, 2)}`);
          return resolve(parseInt(result.feed.entry[0].link[0].$.length, 10));
        });
      }
    );
  });
}


function download(config, dir, writer, callback) {
  console.log(`Downloading ${config.baseDownloadUrl}...`);
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
function downloadDataFile(baseURL, filename, dir, size) {
  return new Promise((resolve, reject) => {
    const fullZipFilePath = path.join(dir, filename);
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
        console.log('Download progress: 100%');
        resolve(fullZipFilePath);
      });
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
            error => {
              if (error) throw error;
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

function unzip(config, dir, writer, callback) {
  console.log('WARNING, make sure you have at least 45 Gb of free disk space for extraction, or press Ctrl-c to abort.');
  console.log('The unzip phase itself can take up to an hour and will extract about 4.000 XML files.');
  console.log('Since the zipfile consists of sub-zipfiles of unknown size, there cannot be given an estimation of remaining time.');
  console.log('The process will appear to be frozen for quite some time, especially on the ***PND***.zip file.');
  console.log('However, this will at least spare you the logging of about 4000 file names.');
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

function mkdir(path) {
  return new Promise((resolve, reject) => {
    mkdirp(path, err => {
      if (err) {
        console.log(`Error during directory creation: ${err}`);
        reject(err);
      }
      resolve();
    });
  });
}


function mapFilesToJobs(dir, extractDir) {
  const fileTypes = {
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
    },
    WPL: {
      converter: placesWorkers,
      outputPITsFile: 'woonplaats.pits.ndjson',
      outputRelationsFile: 'woonplaats.relations.ndjson'
    }
  };

  return fs.readdirSync(dir)
    .filter(file => file.slice(-4) === '.xml')
    .map(file => {
      const type = file.slice(4, 7);
      const job = {};
      if (!fileTypes[type]) return null;
      job.converter = fileTypes[type].converter;
      job.inputFile = path.resolve(path.join(dir, file));
      job.outputPITsFile = path.resolve(path.join(extractDir, fileTypes[type].outputPITsFile));
      job.outputRelationsFile = path.resolve(path.join(extractDir, fileTypes[type].outputRelationsFile));
      return job;
    })
    .filter(job => (job));
}

function convert(config, dir, writer, callback) {
  const extractDir = path.join(config.data.generatedDataDir, 'data-bag');
  console.log('WARNING, make sure you have at least 45 Gb of free disk space for conversion, or press Ctrl-c to abort.');
  const jobs = mapFilesToJobs(dir, extractDir);

  mkdir(extractDir)
    .then(() => {
      const jobStream = highland(jobs);

      jobStream
        .map(job => {
          console.log(`Processing ${job.inputFile} to output to ${job.outputPITsFile} and ${job.outputRelationsFile}`);
          return highland(wrapJob(job.converter.extractFromFile, job.inputFile, job.outputPITsFile, job.outputRelationsFile));
        })
        .parallel(NUM_CPUS - 1)
        .errors(err => {
          fs.appendFileSync(path.join(__dirname, 'error.log'), JSON.stringify(err));
          return console.log('Stream threw error. Wrote error to error.log.');
        })
        .toArray(result => {
          console.log('Done processing all files!');
          return callback(null, result)
        });

    })
    .catch(err => callback(err, null));
}

function wrapJob(jobFunction, sourceFile, pitsFile, relationsFile) {
  return new Promise((resolve, reject) => {
    jobFunction(sourceFile, pitsFile, relationsFile, (err, result) => {
      if (err) return reject(err);
      return resolve(result);
    });
  });
}

module.exports = {
  download,
  extractDownloadSize,
  downloadDataFile,
  unzip,
  extractZipfile,
  convert,
  mapFilesToJobs,
  mkdir,
  steps: [
    download,
    unzip,
    convert
  ]
};
