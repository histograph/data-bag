var fs = require('fs');
var path = require('path');
var Promise = require('bluebird');
var request = require('request');
var yauzl = require('yauzl');
var mkdirp = require('mkdirp');
var highland = require('highland');

// The whole XML misery
var sax = require('sax'),
    strict = true, // set to false for html-mode
    parser = sax.parser(strict);
var saxpath = require('saxpath');
var xml2js = require('xml2js');

var bagconfig = require('./config.json');

// ==================================== API ====================================


module.exports = {
    title : 'BAG',
    url : 'http://bag.kadaster.nl',
    downloadDataFile : downloadDataFile,
    extractZipfile : extractZipfile,
    listBuildingFiles : listBuildingFiles,
    extractBuildingsFromFile : extractBuildingsFromFile,
    joinGMLposlist : joinGMLposlist,
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


function listBuildingFiles(dir){
    return fs.readdirSync(dir).filter(function(file){
        return file.slice(-4) !== ".zip" && file.search("PND") > 0;
    })
}

function extractBuildingsFromDir(dir, targetFile){
    return new Promise(function(resolve, reject){
        filesToProcess = listBuildingFiles(dir);
        highland(filesToProcess).map(function(file) {
            extractBuildingsFromFile(file, targetFile)
                .then(function (buildings) {
                    highland(buildings).each(function (building) {
                        //append to form ndjson
                        fs.appendFileSync(targetFile, JSON.stringify(building));
                        resolve(true);
                    });
                });
        })
        .series();
    });
}

function extractBuildingsFromFile(file, targetFile, callback){
    var buildings = [];
    var parser = new xml2js.Parser();

    var saxStream = sax.createStream(strict);
    fs.createReadStream(file, { encoding: 'utf8' })
        .pipe(saxStream);

    var streamer   = new saxpath.SaXPath(saxStream, '//bag_LVC:Pand');

    streamer.on('match', function(xml) {
        parser.parseString(xml, function (err, result) {
            if (!err) {
                buildings.push({
                    uri: module.exports.url + '/pand/' + result["bag_LVC:Pand"]["bag_LVC:identificatie"][0],
                    id: result["bag_LVC:Pand"]["bag_LVC:identificatie"][0],
                    bouwjaar: result["bag_LVC:Pand"]["bag_LVC:bouwjaar"][0],
                    geometrie: joinGMLposlist(result["bag_LVC:Pand"]["bag_LVC:pandGeometrie"][0]["gml:Polygon"][0]["gml:exterior"][0]["gml:LinearRing"][0]["gml:posList"][0]["_"])
                });
            } else {
                console.error("Error parsing xml element", xml, "\n", err);
            }
        });
    });

    saxStream.on("error", function (e) {
        // unhandled errors will throw, since this is a proper node event emitter.
        console.error("error!", e);
        // clear the error
        this._parser.error = null;
        this._parser.resume()
    });
    saxStream.on('end', function(){
        return callback(buildings);
    });

}

function joinGMLposlist(posList) {
    posList = posList.split(' ');

    var geojsonPosList = [];
    var counter = 0;

    while (posList.length !== 0) {
        geojsonPosList[counter] = [];
        geojsonPosList[counter].push(posList.shift());
        geojsonPosList[counter].push(posList.shift());
        geojsonPosList[counter].push(posList.shift());
        counter += 1;
    }

    return geojsonPosList;
}
function convert(config, dir, writer, callback) {
}
