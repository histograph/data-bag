var fs = require('fs');
var path = require('path');
var H = require('highland');

var chai = require("chai");
var chaiAsPromised = require("chai-as-promised");
chai.use(chaiAsPromised);
var should = chai.should();
var assert = chai.assert;
var expect = chai.expect;

var bag = require('../bag.js');
var config = require('../config.json');

describe('histograph-data-bag', function(){
    this.timeout(800000);

    it('should download the test dataset', function shouldDownloadTestdata(){
        return bag.downloadDataFile(config.baseUrlTest, config.dataFileNameTest, __dirname)
            .then(function onFullFilled(){
                return assert.doesNotThrow(function checkForFile(){
                    console.log("Data test file stats:", JSON.stringify(fs.lstatSync(path.join(__dirname, config.dataFileNameTest)), null, 2), "\n");
                });
            });
    });

    it('should extract the zip', function shouldExtractZip(done){
        assert.doesNotThrow(function(){
            console.log('__dirname:', __dirname);
            filename = path.join(__dirname, config.dataFileNameTest);
            var extractedFiles = [];
            bag.extractZipfile(filename, path.join(__dirname, 'unzip'), function(){
                done();
            });
        });
    });

    it('should invalidate an invalid feature', () => {
        var invalidFeature = {
            "type": "Feature",
            "properties": {name: 'My non-simple hourglass-shaped geometry'},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            5.614013671875,
                            52.47608904123904
                        ],
                        [
                            6.35009765625,
                            52.93539665862318
                        ],
                        [
                            6.8939208984375,
                            52.13011607781287
                        ],
                        [
                            7.239990234375,
                            52.65639394198803
                        ],
                        [
                            5.614013671875,
                            52.47608904123904
                        ]
                    ]
                ]
            }
        };

        return bag.validateCoords(invalidFeature.geometry.coordinates, invalidFeature.geometry.type)
            .then(valid => expect(valid).to.be.false)
            .catch(errs => {
                console.error("Validation errors:", errs);
                return expect(errs).to.be.not.null;
            })
    });

    it('should reproject the coordinates to WGS84', () => {
        var geojson = {
            "uri": "http://bag.kadaster.nl/pand/0362100100084298",
            "id": "0362100100084298",
            "bouwjaar": "2011",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            117283.951,
                            475941.101
                        ],
                        [
                            117284.742,
                            475944.408
                        ],
                        [
                            117280.949,
                            475945.315
                        ],
                        [
                            117280.344,
                            475942.787
                        ],
                        [
                            117283.951,
                            475941.101
                        ]
                    ]
                ]
            }
        };

        expect(bag.toWGS84(geojson.geometry.coordinates[0][0])).to.deep.equal([ 4.834646702778442, 52.27019375226181 ]);

    });

    it('should join a gml-extracted position list to a WGS84 geojson-compatible one', () => {
        var testPosList = "116938.595 477068.148 0.0 116930.644 477071.854 0.0 116928.365 477066.959 0.0 116936.316 477063.253 0.0 116936.327 477063.277 0.0 116938.595 477068.148 0.0";
        var outcome = [ [
            [ 116938.595, 477068.148],
            [ 116930.644, 477071.854],
            [ 116928.365, 477066.959],
            [ 116936.316, 477063.253],
            [ 116936.327, 477063.277],
            [ 116938.595, 477068.148]
        ] ];

        return bag.joinGMLposlist(testPosList, "Polygon")
            .then(geojsoncoords => {
                console.log(JSON.stringify(geojsoncoords, null, 2));
                return bag.validateCoords(geojsoncoords, "Polygon")
                    .then(valid => expect(valid).to.be.true)
                    .catch(err => {
                        console.log("geometry validation error:", err.stack);
                        return expect(err).to.be.null
                    });
            })

    });

    it('should extract the building entries from a file', () => {
        var extractedBuildingsFile = path.join(__dirname, "buildings.ndjson");

        return bag.extractBuildingsFromFile(path.join(__dirname, "bag-PND-snippet.xml"))
            .then(buildings => {
                console.log("result length:", buildings.length, "\n");
                console.log("extractedBuildingsFile number 19:", JSON.stringify(buildings[18], null, 2), "\n");

                return expect(buildings[18]).to.deep.equal({
                    "uri": "http://bag.kadaster.nl/pand/0362100100084298",
                    "id": "0362100100084298",
                    "bouwjaar": "2011",
                    "geometry": {
                        "coordinates": [[
                            [
                                4.8346467,
                                52.2701938
                            ],
                            [
                                4.8346579,
                                52.2702235
                            ],
                            [
                                4.8346023,
                                52.2702314
                            ],
                            [
                                4.8345937,
                                52.2702087
                            ],
                            [
                                4.8346467,
                                52.2701938
                            ]
                        ]],
                        "type": "Polygon"
                    }
                });
            });
    });

    it('should write all the data as ndjson', () => {
        var extractedBuildingsFile = path.join(__dirname, "buildings.ndjson");
        if (fs.existsSync(extractedBuildingsFile)) fs.unlinkSync(extractedBuildingsFile);

        return bag.extractBuildingsFromFile(path.join(__dirname, "bag-PND-snippet.xml"))
            .then(buildings => {
                H(buildings)
                    .each(building => {
                        bag.write(building, extractedBuildingsFile);
                    })
                    .done(() => {
                        return expect(fs.existsSync(extractedBuildingsFile)).to.be.true;
                    });
            });

    });

    it('should find the building files', () => {
        var buildingfiles = bag.listBuildingFiles(path.join(__dirname, 'unzip'));
        console.log("Files:", buildingfiles);
        return expect(buildingfiles).to.deep.equal(
            [
                path.join(__dirname, 'unzip','1050PND08032011-01022011-0001.xml'),
                path.join(__dirname, 'unzip', '1050PND08032011-01022011-0002.xml'),
                path.join(__dirname, 'unzip', '1050PND08032011-01022011-0003.xml')
            ]
        );
    });

  it('should extract the building entries from all files in about 4 minutes', () => {
    var extractedBuildingsFile = path.join(__dirname, 'buildings.ndjson');
    if (fs.existsSync(extractedBuildingsFile)) fs.unlinkSync(extractedBuildingsFile);

    return bag.extractBuildingsFromDir(path.join(__dirname, 'unzip'), extractedBuildingsFile)
      .then( buildings => {
        console.log('Result:', buildings);
        return assert(buildings, true);
      })
  })

});