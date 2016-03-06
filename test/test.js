'use strict';
var fs = require('fs');
var path = require('path');
var H = require('highland');

var nock = require('nock');

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
var should = chai.should();
var assert = chai.assert;
var expect = chai.expect;

var bag = require('../bag.js');
var buildingsExtractor = require('../helpers/buildingsextractor.js');
var addressesExtractor = require('../helpers/addressextractor.js');
var config = require('../config.json');

var mockedAtomXML = path.join(__dirname, 'mockups', 'atom_inspireadressen.xml');

describe('histograph-data-bag', function describeTests() {
  describe('download phase', function() {
    it('extracts the dataset size from the source description', () => {
      console.log(mockedAtomXML);
      nock('http://geodata.nationaalgeoregister.nl')
        .defaultReplyHeaders({ 'Content-Type': 'text/xml' })
        .get('/inspireadressen/atom/inspireadressen.xml')
        .replyWithFile(200, mockedAtomXML);

      return bag.extractDownloadSize(config.feedURL)
        .then(size => expect(size).to.equal(1550788857));
    });

    nock('http://data.nlextract.nl')
      .get('/bag/bron/BAG_Amstelveen_2011feb01.zip')
      .replyWithFile(200, mockedAtomXML);

    bag.downloadDataFile(config.baseUrlTest, config.dataFileNameTest, __dirname, 5746696)
      .then(filename => {
        console.log(`Got ${filename}`);
        return expect(fs.lstatSync(filename)).to.not.throw;
      });

  });

  describe('unzip phase', function () {
    this.timeout(30000);
    it('extract the test dataset', done => {
      var unzipDir = path.resolve('./test/unzip');
      var filename = path.resolve('./test/BAG_Amstelveen_2011feb01.zip');

      console.log(`Unzipping to ${unzipDir}`);

      bag.extractZipfile(filename, unzipDir)
        .then(() => {
          expect(fs.readdirSync(unzipDir)).to.deep.equal([
            '1050LIG08032011-01022011.xml',
            '1050LIG08032011-01022011.zip',
            '1050NUM08032011-01022011-0001.xml',
            '1050NUM08032011-01022011-0002.xml',
            '1050NUM08032011-01022011-0003.xml',
            '1050NUM08032011-01022011.zip',
            '1050OPR08032011-01022011.xml',
            '1050OPR08032011-01022011.zip',
            '1050PND08032011-01022011-0001.xml',
            '1050PND08032011-01022011-0002.xml',
            '1050PND08032011-01022011-0003.xml',
            '1050PND08032011-01022011.zip',
            '1050STA08032011-01022011.xml',
            '1050STA08032011-01022011.zip',
            '1050VBO08032011-01022011-0001.xml',
            '1050VBO08032011-01022011-0002.xml',
            '1050VBO08032011-01022011-0003.xml',
            '1050VBO08032011-01022011-0004.xml',
            '1050VBO08032011-01022011.zip',
            '1050WPL08032011-01022011.xml',
            '1050WPL08032011-01022011.zip',
            '1050XXX08032011-01022011.zip',
            'Leveringsdocument-BAG-Extract.xml'
          ]);
          console.log('Test done \n');
          done();
        });
    });
  });

  describe('conversion phase', function () {
    describe('geometry checking functions', function () {
      it('should invalidate an invalid feature', () => {
        var invalidFeature = {
          type: 'Feature',
          properties: { name: 'My non-simple hourglass-shaped geometry' },
          geometry: {
            type: 'Polygon',
            coordinates: [
              [
                [5.6, 52.4],
                [6.3, 52.9],
                [6.8, 52.1],
                [7.2, 52.6],
                [5.6, 52.4]
              ]
            ]
          }
        };

        return buildingsExtractor.validateCoords(invalidFeature.geometry.coordinates, invalidFeature.geometry.type)
          .then(valid => expect(valid).to.be.false)
          .catch(errs => {
            console.error('Validation errors:', errs);
            return expect(errs).to.be.not.null;
          });
      });

      it('should reproject the coordinates to WGS84', () => {
        var geojson = {
          uri: 'http://bag.kadaster.nl/pand/0362100100084298',
          id: '0362100100084298',
          bouwjaar: '2011',
          geometry: {
            type: 'Polygon',
            coordinates: [
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

        expect(buildingsExtractor.toWGS84(geojson.geometry.coordinates[0][0])).to.deep.equal([4.834646702778442, 52.27019375226181]);

      });

      it('should join a gml-extracted position list to a WGS84 geojson-compatible one', () => {
        var testPosList = '116938.595 477068.148 0.0 ' +
          '116930.644 477071.854 0.0 ' +
          '116928.365 477066.959 0.0 ' +
          '116936.316 477063.253 0.0 ' +
          '116936.327 477063.277 0.0 ' +
          '116938.595 477068.148 0.0';

        return buildingsExtractor.joinGMLposlist(testPosList, 'Polygon')
          .then(geojsoncoords => {
            console.log(JSON.stringify(geojsoncoords, null, 2));
            return buildingsExtractor.validateCoords(geojsoncoords, 'Polygon')
              .then(valid => expect(valid).to.be.true)
              .catch(err => {
                console.log('geometry validation error:', err.stack);
                return expect(err).to.be.null;
              });
          });

      });

    });

    describe('buildings extraction', function () {
      it('should extract the building entries from a file', (done) => {
        var extractedBuildingsFile = path.join(__dirname, 'buildings.ndjson');

        buildingsExtractor.extractBuildingsFromFile(path.join(__dirname, 'mockups', 'bag-PND-snippet.xml'), (err, buildings) => {
          if (err) throw err;

          console.log('result length:', buildings.length, '\n');
          console.log('extractedBuildingsFile number 19:', JSON.stringify(buildings[18], null, 2), '\n');

          expect(buildings[18]).to.deep.equal({
            uri: 'http://bag.kadaster.nl/pand/0362100100084298',
            id: '0362100100084298',
            bouwjaar: '2011',
            startDate: '2011010500000000',
            endDate: null,
            geometry: {
              coordinates: [[
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
              type: 'Polygon'
            }
          });
          done();
        });
      });

      it('should find the building files', () => {
        var buildingfiles = bag.listBuildingFiles(path.join(__dirname, 'unzip'));
        console.log('Files:', buildingfiles);
        return expect(buildingfiles).to.deep.equal(
          [
            path.join(__dirname, 'unzip', '1050PND08032011-01022011-0001.xml'),
            path.join(__dirname, 'unzip', '1050PND08032011-01022011-0002.xml'),
            path.join(__dirname, 'unzip', '1050PND08032011-01022011-0003.xml')
          ]
        );
      });

    });

    describe('addresses extraction', () => {
      it('should find the addresses files', () => {
        var addressfiles = bag.listAddressFiles(path.join(__dirname, 'unzip'));
        console.log('Files:', addressfiles);
        return expect(addressfiles).to.deep.equal(
          [
            path.join(__dirname, 'unzip', '1050NUM08032011-01022011-0001.xml'),
            path.join(__dirname, 'unzip', '1050NUM08032011-01022011-0002.xml'),
            path.join(__dirname, 'unzip', '1050NUM08032011-01022011-0003.xml')
          ]
        );
      });

      var extractedAddressesFile = path.join(__dirname, 'addresses.ndjson');

      it('should extract an address from a mocked snippet', done => {
        addressesExtractor.extractAddressesFromFile(path.join(__dirname, 'mockups', 'bag-NUM-snippet.xml'), (err, addressNodes, addressEdges) => {
          if (err) throw err;

          console.log(`Result: ${addressNodes.length} addresses, ${addressEdges.length} related streets \n`);

          expect(addressNodes[1]).to.deep.equal({
            endDate: null,
            huisletter: null,
            huisnummer: '12',
            id: '0957200000300090',
            postcode: '6041LZ',
            startDate: '2010112200000000',
            uri: 'http://bag.kadaster.nl/nummeraanduiding/0957200000300090'
          });

          expect(addressEdges[0]).to.deep.equal({
            from: 'http://bag.kadaster.nl/nummeraanduiding/0957200000300090',
            to: 'http://bag.kadaster.nl/openbareruimte/0957300000174823',
            type: 'hg:related'
          });

          done();
        });
      });
    });

  });


  /*
   this.timeout(140000);
   it('should extract the building entries from all files in about 2 minutes', () => {
   var extractedBuildingsFile = path.join(__dirname, 'buildings.ndjson');
   var unzipDir = path.join(__dirname, 'unzip').toString();

   if (fs.existsSync(extractedBuildingsFile)) fs.unlinkSync(extractedBuildingsFile);

   return expect(bag.extractBuildingsFromDir(unzipDir, extractedBuildingsFile)).to.be.fulfilled;
   });
   */

});
