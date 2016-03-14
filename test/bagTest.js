'use strict';
var fs = require('fs');
var path = require('path');
var rimraf = require('rimraf');
var nock = require('nock');

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
var expect = chai.expect;

var bag = require('../bag.js');
var config = require('./mockups/config.json');
var mockedAtomXML = path.join(__dirname, 'mockups', 'atom_inspireadressen.xml');

describe('histograph-data-bag', function () {
  describe('download phase', () => {
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

    it('should map the files to a list of jobs', done => {
      expect(jobs.length).to.equal(7);
      expect(jobs[0].inputFile.split('.').slice(-1)[0]).to.deep.equal('xml');
      expect(jobs[0].outputPITsFile.split('.').slice(-2)[0]).to.deep.equal('pits');
      expect(jobs[0].outputRelationsFile.split('.').slice(-2)[0]).to.deep.equal('relations');
      done();
    });

    it('should create the extraction dir if it does not exist', () => {
      var extractDir = path.join(__dirname, 'extract');
      if (fs.existsSync(extractDir)) rimraf(extractDir, () => {
        return bag.mkdir(extractDir).then(result => {
          return expect(fs.existsSync(extractDir)).to.equal(true);
        });
      });

    });

    this.timeout(100000);

    it('should extract the entries from a list of files', done => {
      var sourceDir = path.join(__dirname, 'unzip');
      bag.convert(config, sourceDir, null, (err, result) => {
        if (err) throw err;
        expect(err).to.equal(null);
        expect(result).to.equal(true);
        done();
      });
    });

  });

});
