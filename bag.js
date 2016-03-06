'use strict';

var fs          = require('fs');
var path        = require('path');
var Promise     = require('bluebird');
var request     = require('request');
var progress    = require('request-progress');
var yauzl       = require('yauzl');
var mkdirp      = require('mkdirp');
var highland    = require('highland');
var GJV         = require('geojson-validation');
var proj4       = require('proj4');
var reproject   = require('reproject');

var sax         = require('sax');
var saxpath     = require('saxpath');
var xml2js      = require('xml2js');

var jsts        = require('jsts');
var reader      = new jsts.io.GeoJSONReader();

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
  [
    'extractBuildingsFromFile',
    'validateCoords',
    'joinGMLposlist',
    'isValidGeoJSON',
    'toWGS84'
  ]
);

var addressworkers = workerFarm(
  FARM_OPTIONS,
  require.resolve('./helpers/addressextractor.js'),
  [
    'extractBuildingsFromFile',
    'validateCoords',
    'joinGMLposlist',
    'isValidGeoJSON',
    'toWGS84'
  ]
);

var config          = require('../config/index.js');

module.exports      = {
  extractDownloadSize: extractDownloadSize,
  downloadDataFile: downloadDataFile,
  extractZipfile: extractZipfile,
  listBuildingFiles: listBuildingFiles,
  extractBuildingsFromDir: extractBuildingsFromDir,
  extractAddressesFromDir: extractAddressesFromDir,
  listAddressFiles: listAddressFiles,
  steps: [
    download,
    unzip,
    convert
  ]
};

function download(config, dir, writer, callback) {
  console.log(`Downloading...`);
  var size = 1550788857;
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

function convert(config, dir, writer, callback) {
  var extractDir = path.join(config.data.generatedDataDir, 'data-bag');
  extractDir = path.resolve(extractDir);

  extractBuildingsFromDir(dir, path.join(extractDir, 'pand.ndjson'))
    .then(() => extractAddressesFromDir(dir, path.join(extractDir, 'adres.ndjson')))
    .then(() => callback)
    .catch((error) => callback(error));
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

function extractBuildingsFromDir(dir, targetFile) {
  return new Promise((resolve, reject) => {
    var writeStream = fs.createWriteStream(targetFile);
    var buildingsStream = highland(listBuildingFiles(dir));
    var wrappedExtractor = highland.wrapCallback(buildingsworkers.extractBuildingsFromFile);

    buildingsStream.map(file => {
      console.log(`Extracting buildings from file ${file} \n`);
      return highland(wrappedExtractor(file));
    })
      .parallel(NUM_CPUS - 1) //leave some juice
      .sequence() //Flatten one level deep
      .errors(err => {
        fs.writeFileSync(path.join(__dirname, 'error.log'), JSON.stringify(err));
        return console.log(`Buildings stream threw error. Wrote error to error.log.`);
      })
      .map(building => JSON.stringify(building) + '\n')
      .pipe(writeStream);

    writeStream.on('finish', () => resolve());
    writeStream.on('error', error => reject(error));
  });
}

function listBuildingFiles(dir) {
  return fs.readdirSync(dir)
    .filter(file => file
      .slice(-4) !== '.zip' && file.search('PND') > 0)
      .map(file => path.join(dir, file)
      );
}

function extractAddressesFromDir(dir, targetFile) {
  return new Promise((resolve, reject) => {
    var writeStream = fs.createWriteStream(targetFile);
    var adressesStream = highland(listAddressFiles(dir));
    var wrappedExtractor = highland.wrapCallback(addressworkers.extractAddressesFromFile);

    adressesStream.map(file => {
      console.log(`Extracting addresses from file ${file} \n`);
      return highland(wrappedExtractor(file));
    })
      .parallel(NUM_CPUS - 1) //leave some juice
      .sequence() //Flatten one level deep
      .errors(err => {
        fs.writeFileSync(path.join(__dirname, 'error.log'), JSON.stringify(err));
        return console.log(`Addresses stream threw error. Wrote error to error.log.`);
      })
      .map(building => JSON.stringify(building) + '\n')
      .pipe(writeStream);

    writeStream.on('finish', () => resolve());
    writeStream.on('error', error => reject(error));
  });
}

function listAddressFiles(dir) {
  return fs.readdirSync(dir)
    .filter(file => file
      .slice(-4) !== '.zip' && file.search('NUM') > 0)
      .map(file => path.join(dir, file)
      );
}
