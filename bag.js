'use strict';

var
    fs = require('fs'),
    path = require('path'),
    Promise = require('bluebird'),
    request = require('request'),
    yauzl = require('yauzl'),
    mkdirp = require('mkdirp'),
    highland = require('highland'),
    GJV = require('geojson-validation'),
    jsts = require('jsts'),
    proj4 = require('proj4'),
    reproject = require('reproject');

// The whole XML misery
var sax = require('sax'),
    strict = true, // set to false for html-mode
    parser = sax.parser(strict);
var saxpath = require('saxpath'),
    xml2js = require('xml2js');

var bagconfig = require('./config.json');

var geometryFactory = new jsts.geom.GeometryFactory();
var reader = new jsts.io.GeoJSONReader();

// ==================================== API ====================================


module.exports = {
    title : 'BAG',
    url : 'http://bag.kadaster.nl',
    downloadDataFile : downloadDataFile,
    extractZipfile : extractZipfile,
    listBuildingFiles : listBuildingFiles,
    extractBuildingsFromDir : extractBuildingsFromDir,
    extractBuildingsFromFile : extractBuildingsFromFile,
    validateCoords : validateCoords,
    joinGMLposlist : joinGMLposlist,
    toWGS84 : toWGS84,
    write : write,
    steps : [
        download,
        convert
    ]
};

function download(config, dir, writer, callback, fileDownloadURL) {
    downloadDataFile(bagconfig.baseDownloadUrl, bagconfig.datafilename, dir)
        .then(function() {
            return extractZipfile(path.join(dir, bagconfig.datafilename), dir)
        })
        .then(function(){
                console.log(Date.now(), 'extraction of ', bagconfig.baseDownloadUrl + bagconfig.datafilename, 'complete!')
            }, function (error){
                console.error(Date.now(), 'Download and extraction failed')
            }
        );
}


function downloadDataFile(baseURL, filename, dir) {
    return new Promise(function (resolve, reject){
        request
            .get(baseURL + filename)
            .pipe(fs.createWriteStream(path.join(dir, filename)))
            .on('error', function(err) {
                reject(err);
            })
            .on('finish', function() {
                resolve()
            });
    })
}


function extractZipfile(zipfilename, extractdir, callback) {
    console.log('extractdir: ', extractdir, '\n');
    mkdirp(extractdir);
    console.log('zipfilename: ', zipfilename, '\n');
    yauzl.open(zipfilename, {lazyEntries: true}, function(err, zipfile) {
        if (err) throw err;
        zipfile.readEntry();
        zipfile.on("entry", function(entry) {
            if (/\/$/.test(entry.fileName)) {
                // directory file names end with '/'
                mkdirp(entry.fileName, function(err) {
                    if (err) throw err;
                    zipfile.readEntry();
                });
            } else {
                // file entry
                zipfile.openReadStream(entry, function(err, readStream) {
                    if (err) throw err;
                    // ensure parent directory exists
                    mkdirp(path.dirname(entry.fileName), function(err) {
                        if (err) throw err;
                        readStream.pipe(fs.createWriteStream(path.join(extractdir, entry.fileName)));

                        readStream.on('end', function() {
                            if (entry.fileName.slice(-4) === '.zip'){
                                extractZipfile(path.join(extractdir, entry.fileName), extractdir, function(){
                                    console.log('Extracted subzip');
                                    zipfile.readEntry();
                                });
                            } else {
                                zipfile.readEntry();
                            }
                        });

                        readStream.on('error', function(err){
                            console.log(Date.now(), err.stack);
                            throw(err);
                        })
                    });
                });
            }
        });
        zipfile.on('end', callback);
    });
}


function extractBuildingsFromDir(dir, targetFile){
  return new Promise(function(resolve, reject){
    var writeStream = fs.createWriteStream(targetFile);

    var buildingsStream = highland(listBuildingFiles(dir));
    buildingsStream.map(file => {
        console.log(`Extracting buildings from file ${file} \n`);
        return highland(extractBuildingsFromFile(file));
      })
      .parallel(10) //Do max 10 files at once
      .sequence() //Flatten one level deep
      .map(building => {
        return JSON.stringify(building) + '\n';
      })
      .pipe(writeStream);

    writeStream.on('finish', () => resolve(true));
  });
}


function listBuildingFiles(dir){
    return fs.readdirSync(dir).filter(file => {
        return file.slice(-4) !== ".zip" && file.search("PND") > 0;
    }).map(file => path.join(dir, file))
}


function extractBuildingsFromFile(file, targetFile){
    return new Promise( (resolve, reject) => {
        var buildings = [];
        var parser = new xml2js.Parser();

        var saxStream = sax.createStream(strict);
        fs.createReadStream(file, { encoding: 'utf8' })
            .pipe(saxStream);

        var streamer   = new saxpath.SaXPath(saxStream, '//bag_LVC:Pand');

        streamer.on('match', xml => {
            parser.parseString(xml, (err, result) => {
                if (!err) {
                    joinGMLposlist(result["bag_LVC:Pand"]["bag_LVC:pandGeometrie"][0]["gml:Polygon"][0]["gml:exterior"][0]["gml:LinearRing"][0]["gml:posList"][0]["_"])
                        .then(list => {
                            var polygon = [];
                            polygon[0] = list;

                            buildings.push({
                                uri: module.exports.url + '/pand/' + result["bag_LVC:Pand"]["bag_LVC:identificatie"][0],
                                id: result["bag_LVC:Pand"]["bag_LVC:identificatie"][0],
                                bouwjaar: result["bag_LVC:Pand"]["bag_LVC:bouwjaar"][0],
                                geometry: {
                                    type: "Polygon",
                                    coordinates: polygon
                                }
                            })
                        });
                } else {
                    console.error("Error parsing xml element", xml, "\n", err);
                }
            });
        });

        saxStream.on("error", function (err) {
            console.error("saxStream threw error", err.stack);
            // clear the error
            this._parser.error = null;
            this._parser.resume()
        });
        saxStream.on('end', function(){
            resolve(buildings);
        });

    })
}

function joinGMLposlist(posList, type) {
    return new Promise( (resolve, reject) => {
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

        if (type === "Polygon") {
            //add extra level of array
            var extralevel = [[[]]];
            extralevel[0] = geojsonPosList;
            resolve(extralevel);

        }
        resolve(geojsonPosList);

    });
}

function validateCoords(geojsoncoords, type) {
    return new Promise( (resolve, reject) => {
        var geojson = {
            type: "Feature",
            geometry: {
                type: type,
                coordinates: geojsoncoords
            },
            properties: {}
        };

        GJV.isFeature(geojson, (valid, errs) => {
            if (!valid) {
                console.error("Validator rejecting geometry due to:", errs);
                reject(errs);
            } else {
                console.log("JSTS evaluated feature as valid: " + isValidGeoJSON(geojson) + "\n");
                resolve(isValidGeoJSON(geojson));
            }
        });
    });
}

function isValidGeoJSON(geoJSONPolygon){
    var jstsGeometry  = reader.read(geoJSONPolygon.geometry);

    if (jstsGeometry) {
        var validator = new jsts.operation.valid.IsValidOp(jstsGeometry);
        return validator.isValid();
    }
}

function write(data, file) {
    console.log("Wrote some data\n");
    fs.appendFileSync(file, JSON.stringify(data) + "\n");
}

function toWGS84(point) {
    var EPSG28992 = "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.417,50.3319,465.552,-0.398957,0.343988,-1.8774,4.0725 +units=m +no_defs";

    return proj4(
        EPSG28992,
        proj4("WGS84")
    ).forward(point);
}

function convert(config, dir, writer, callback) {
}
