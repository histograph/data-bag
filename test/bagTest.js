'use strict';
const fs = require('fs');
const path = require('path');
const rimraf = require('rimraf');
const nock = require('nock');

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const expect = chai.expect;

const bag = require('../bag.js');
const config = require('./mockups/config.json');
const mockedAtomXML = path.join(__dirname, 'mockups', 'atom_inspireadressen.xml');
const extractDir = path.join(__dirname, 'extract');

describe('histograph-data-bag', function bagTest() {
  describe('download phase', function download() {
    it('extracts the dataset size from the source description', () => {
      nock('http://geodata.nationaalgeoregister.nl')
        .defaultReplyHeaders({ 'Content-Type': 'text/xml' })
        .get('/inspireadressen/atom/inspireadressen.xml')
        .replyWithFile(200, mockedAtomXML);

      return bag.extractDownloadSize(config.feedURL)
        .then(size => expect(size).to.equal(1550788857));
    });

    it('downloads the file', () => {
      nock('http://data.nlextract.nl')
        .get('/bag/bron/BAG_Amstelveen_2011feb01.zip')
        .replyWithFile(200, mockedAtomXML);

      return bag.downloadDataFile(config.baseUrlTest, config.dataFileNameTest, __dirname, 5746696)
        .then(filename => expect(fs.lstatSync(filename)).to.not.throw);
    });
  });

  describe('unzip phase', function unzip() {
    this.timeout(30000);
    it('extract the test dataset', () => {
      const unzipDir = path.resolve('./test/unzip');
      const filename = path.resolve('./test/BAG_Amstelveen_2011feb01.zip');

      return bag.extractZipfile(filename, unzipDir)
        .then(() => {
          return expect(fs.readdirSync(unzipDir)).to.deep.equal([
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
        });
    });
  });

  describe('conversion phase', function conversion() {
    let jobs;

    before('create jobs object', () => {
      jobs = bag.mapFilesToJobs('./test/unzip', './test');
    });

    after('cleanup', () => {
      console.log('Cleaning up');
      rimraf.sync(extractDir);
    });

    it('should map the files to a list of jobs', done => {
      expect(jobs.length).to.equal(8);
      expect(jobs[0].inputFile.split('.').slice(-1)[0]).to.deep.equal('xml');
      expect(jobs[0].outputPITsFile.split('.').slice(-2)[0]).to.deep.equal('pits');
      expect(jobs[0].outputRelationsFile.split('.').slice(-2)[0]).to.deep.equal('relations');
      done();
    });

    it('should create the extraction dir if it does not exist', () => bag.mkdir(extractDir)
      .then(() => expect(fs.existsSync(extractDir)).to.equal(true)));

    this.timeout(200000);

    it('should extract the entries from a list of files', done => {
      const sourceDir = path.join(__dirname, 'unzip');
      bag.convert(config, sourceDir, null, (err, result) => {
        if (err) return done(err);
        expect(err).to.equal(null);
        console.log(result);
        expect(result).to.deep.equal(new Array(8).fill(true));
        done();
      });
    });
  });
});
