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
var publicSpacesExtractor = require('../helpers/publicspacesextractor.js');

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
    var jobs;

    before('create jobs object', () => {
      jobs = bag.mapFilesToJobs('./test/unzip', './test');
    });

    describe('file name mapping to jobs', () => {
      it('should map the files to a list of jobs', done => {
        expect(jobs.length).to.equal(7);
        expect(jobs[0].inputFile.split('.').slice(-1)[0]).to.deep.equal('xml');
        expect(jobs[0].outputPITsFile.split('.').slice(-2)[0]).to.deep.equal('pits');
        expect(jobs[0].outputRelationsFile.split('.').slice(-2)[0]).to.deep.equal('relations');
        done();
      });
    });

    describe('buildings extraction', function() {
      it('should extract the building entries from a file', (done) => {
        buildingsExtractor.extractFromFile(path.join(__dirname, 'mockups', 'bag-PND-snippet.xml'), (err, buildings) => {
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

    });

    describe('addresses extraction', () => {
      it('should extract an address from a mocked snippet', done => {
        addressesExtractor.extractFromFile(path.join(__dirname, 'mockups', 'bag-NUM-snippet.xml'), (err, addressNodes, addressEdges) => {
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

    describe('public spaces extraction', function() {
      it('should extract the public spaces from the snippet', done => {
        publicSpacesExtractor.extractFromFile(path.join(__dirname, 'mockups', 'bag-OPR-snippet.xml'), (err, publicSpaceNodes, publicSpaceEdges) => {
          if (err) throw err;

          console.log(`Result: ${publicSpaceNodes.length} addresses, ${publicSpaceEdges.length} related streets \n`);
          expect(publicSpaceNodes.length).to.equal(3);
          expect(publicSpaceEdges.length).to.equal(3);

          expect(publicSpaceNodes[0]).to.deep.equal({
            uri: 'http://bag.kadaster.nl/openbareruimte/0003300000116985',
            id: '0003300000116985',
            name: 'Abel Eppensstraat',
            startDate: '1956032800000000',
            endDate: null
          });

          expect(publicSpaceEdges[0]).to.deep.equal({
            from: 'http://bag.kadaster.nl/openbareruimte/0003300000116985',
            to: 'http://bag.kadaster.nl/woonplaats/3386',
            type: 'hg:liesIn'
          });

          done();
        });
      });

    });

  });

  this.timeout(140000);
  it('should extract the entries from all files in about 2 minutes', () => {
    var extractedBuildingsFile = path.join(__dirname, 'buildings.ndjson');
    var unzipDir = path.join(__dirname, 'unzip').toString();

    if (fs.existsSync(extractedBuildingsFile)) fs.unlinkSync(extractedBuildingsFile);

    return expect(bag.extractBuildingsFromDir(unzipDir, extractedBuildingsFile)).to.be.fulfilled;
  });

});
