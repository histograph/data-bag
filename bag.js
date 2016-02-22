'use strict';

var fs          = require('fs');
var path        = require('path');
var Promise     = require('bluebird');
var request     = require('request');
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
  require.resolve('./buildingsextractor.js'),
  [
    'extractBuildingsFromFile',
    'validateCoords',
    'joinGMLposlist',
    'isValidGeoJSON',
    'toWGS84'
  ]
);

var bagconfig       = require('./config.json');

module.exports      = {
  title: 'BAG',
  url: 'http://bag.kadaster.nl',
  downloadDataFile: downloadDataFile,
  extractZipfile: extractZipfile,
  listBuildingFiles: listBuildingFiles,
  extractBuildingsFromDir: extractBuildingsFromDir,
  extractBuildingsFromFile: extractBuildingsFromFile,
  validateCoords: validateCoords,
  joinGMLposlist: joinGMLposlist,
  toWGS84: toWGS84,
  write: write,
  steps: [
    download,
    unzip,
    convert
  ]
};

function download(config, dir, writer, callback) {
  return downloadDataFile(config.baseDownloadUrl, bagconfig.datafilename, dir)
    .then((fullPath) => {
      console.log(`${Date.now()} download of ${fullPath} complete!`);
      return callback;
    })
    .catch(error => {
      console.error(`${Date.now()} Download failed due to ${error}`);
      return callback(error);
    });
}

function unzip(config, dir, writer, callback) {
  return extractZipfile(path.join(dir, config.datafilename), dir)
    .then(() => {
      console.log(`${Date.now()} extraction complete!`);
      return callback;
    })
    .catch(error => {
      console.error(`${Date.now()} Extraction failed due to ${error}`);
      return callback(error);
    });
}

function convert(config, dir, writer, callback) {
  var extractDir = path.join(config.data.generatedDataDir, 'data-bag');
  extractDir = path.resolve(extractDir);

  extractBuildingsFromDir(dir, path.join(extractDir, 'pand.ndjson'))
    .then(() => callback)
    .catch((error) => callback(error));
}

function downloadDataFile(baseURL, filename, dir) {
  return new Promise((resolve, reject) => {
    var fullZipFilePath = path.join(dir, filename);

    request
      .get(baseURL + filename)
      .pipe(fs.createWriteStream(fullZipFilePath))
      .on('error', (err) => {
        reject(err);
      })
      .on('finish', () => {
        resolve(fullZipFilePath);
      });
  });
}

function extractZipfile(zipfilename, extractdir) {
  return new Promise((resolve, reject) => {
    console.log('extractdir: ', extractdir, '\n');
    mkdirp(extractdir);

    console.log('zipfilename: ', zipfilename, '\n');
    yauzl.open(zipfilename, {lazyEntries: true}, (err, zipfile) => {
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
  })
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
      .parallel(NUM_CPUS)
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
  return fs.readdirSync(dir).filter(file => {
    return file.slice(-4) !== '.zip' && file.search('PND') > 0;
  }).map(file => path.join(dir, file));
}

function extractBuildingsFromFile(file) {
  return new Promise((resolve, reject) => {
    var buildings = [];
    var parser = new xml2js.Parser();
    var strict = true;

    var saxStream = sax.createStream(strict);
    fs.createReadStream(file, { encoding: 'utf8' })
      .pipe(saxStream);

    var streamer   = new saxpath.SaXPath(saxStream, '//bag_LVC:Pand');

    streamer.on('match', xml => {
      parser.parseString(xml, (err, result) => {
        if (err) {
          console.error(`Error parsing xml element ${xml} \n ${err.stack}`);
          reject(err);
        }

        joinGMLposlist(result['bag_LVC:Pand']['bag_LVC:pandGeometrie'][0]['gml:Polygon'][0]['gml:exterior'][0]['gml:LinearRing'][0]['gml:posList'][0]._)
          .then(list => {
            var polygon = [];
            polygon[0] = list;

            buildings.push({
              uri: module.exports.url + '/pand/' + result['bag_LVC:Pand']['bag_LVC:identificatie'][0],
              id: result['bag_LVC:Pand']['bag_LVC:identificatie'][0],
              bouwjaar: result['bag_LVC:Pand']['bag_LVC:bouwjaar'][0],
              geometry: {
                type: 'Polygon',
                coordinates: polygon
              }
            });
          });

      });
    });

    saxStream.on('error', err => {
      console.error(`saxStream threw error ${err.stack}`);

      // clear the error
      this._parser.error = null;
      this._parser.resume();
    });

    saxStream.on('end', () => {
      resolve(buildings);
    });

  });
}

function joinGMLposlist(posList, type) {
  return new Promise((resolve, reject) => {
    posList = posList.split(' ');

    var geojsonPosList = [];
    var counter = 0;

    while (posList.length !== 0) {
      var point = [];
      point.push(parseFloat(posList.shift()));
      point.push(parseFloat(posList.shift()));
      posList.shift(); // skip 3d height
      point = toWGS84(point)
        .map(coordinate => parseFloat(
          coordinate.toFixed(7))
        );
      geojsonPosList[counter] = point;

      counter += 1;
    }

    if (type === 'Polygon') {
      //add extra level of array
      var extralevel = [[[]]];
      extralevel[0] = geojsonPosList;
      resolve(extralevel);
    }

    resolve(geojsonPosList);

  });
}

function validateCoords(geojsoncoords, type) {
  return new Promise((resolve, reject) => {
    var geojson = {
      type: 'Feature',
      geometry: {
        type: type,
        coordinates: geojsoncoords
      },
      properties: {}
    };

    GJV.isFeature(geojson, (valid, errs) => {
      if (!valid) {
        console.error('Validator rejecting geometry due to:', errs);
        reject(errs);
      } else {
        console.log('JSTS evaluated feature as valid: ' + isValidGeoJSON(geojson) + '\n');
        resolve(isValidGeoJSON(geojson));
      }
    });
  });
}

function isValidGeoJSON(geoJSONPolygon) {
  var jstsGeometry  = reader.read(geoJSONPolygon.geometry);

  if (jstsGeometry) {
    var validator = new jsts.operation.valid.IsValidOp(jstsGeometry);
    return validator.isValid();
  }
}

function write(data, file) {
  console.log('Wrote some data\n');
  fs.appendFileSync(file, JSON.stringify(data) + '\n');
}

function toWGS84(point) {
  var EPSG28992 = '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.417,50.3319,465.552,-0.398957,0.343988,-1.8774,4.0725 +units=m +no_defs';

  return proj4(
    EPSG28992,
    proj4('WGS84')
  ).forward(point);
}
