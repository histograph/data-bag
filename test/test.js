var fs = require('fs');
var path = require('path');
var assert = require('assert');

var bag = require('../bag.js');
var config = require('../config.json');


describe('histograph-data-bag', function(){
    this.timeout(80000);

/*

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
*/

    it('should find the buildings', function shouldFindBuildings(done){
        assert.doesNotThrow(function(){
            buildingfiles = bag.listBuildingFiles(path.join(__dirname, 'unzip'));
            console.log("Files:", buildingfiles);
            done();
        });
    });

    it('should extract the building entries from a file', function(done){
        var extractedBuildingsFile = path.join(__dirname, "buildings.ndjson");
        bag.extractBuildingsFromFile(
            path.join(__dirname, "bag-pand-snippet.xml"),
            extractedBuildingsFile,
            function (buildings){
                console.log("Result:", JSON.stringify(buildings, null, 2));
                assert(buildings, true);
                done();
            })
    });

    it('should join a gml-extracted position list to a geojson-compatible one', function(){
        var testPosList = "116938.595 477068.148 0.0 116930.644 477071.854 0.0 116928.365 477066.959 0.0 116936.316 477063.253 0.0 116936.327 477063.277 0.0 116938.595 477068.148 0.0";
        var outcome = [ [ '116938.595', '477068.148', '0.0' ],
            [ '116930.644', '477071.854', '0.0' ],
            [ '116928.365', '477066.959', '0.0' ],
            [ '116936.316', '477063.253', '0.0' ],
            [ '116936.327', '477063.277', '0.0' ],
            [ '116938.595', '477068.148', '0.0' ] ];

        assert.deepEqual(bag.joinGMLposlist(testPosList), outcome);
    });
/*
    it('should extract the building entries from a file', function(done){
        var extractedBuildingsFile = path.join(__dirname, "buildings.ndjson");
        bag.extractBuildingsWithXmlStream(
            path.join(__dirname, "unzip", "1050PND08032011-01022011-0001.xml"),
            extractedBuildingsFile,
            function (err, buildings){
                console.log("Result:", buildings);
                assert(buildings, true);
                done();
            })
    })
*/

});