'use strict';
const fs = require('fs');
const path = require('path');
const nock = require('nock');

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const expect = chai.expect;

const addressesExtractor = require('../../helpers/addressesextractor.js');
const sourceFile = path.join(__dirname, '..', 'mockups', 'bag-NUM-snippet.xml');
const outputPITsFile = path.join(__dirname, '..', 'adres.pits.ndjson');
const outputRelationsFile = path.join(__dirname, '..', 'adres.relations.ndjson');

describe('addresses extraction', () => {

  after('Cleanup', () => {
    fs.unlinkSync(outputPITsFile);
    fs.unlinkSync(outputRelationsFile);
  });

  it('should extract an address from a mocked snippet', done => {
    addressesExtractor.extractFromFile(sourceFile,  outputPITsFile, outputRelationsFile, (err, result) => {
      if (err) throw err;

      var nodes = fs.readFileSync(outputPITsFile, 'utf-8')
        .split('\n')
        .filter(node => (node))
        .map(node => JSON.parse(node));

      var edges = fs.readFileSync(outputRelationsFile, 'utf-8')
        .split('\n')
        .filter(edge => (edge))
        .map(edge => JSON.parse(edge));

      console.log(`Result: ${nodes.length} addresses, ${edges.length} related streets \n`);

      expect(nodes[1]).to.deep.equal({
        endDate: null,
        huisletter: null,
        huisnummer: '12',
        id: '0957200000300090',
        postcode: '6041LZ',
        startDate: '2010112200000000',
        uri: 'http://bag.kadaster.nl/nummeraanduiding/0957200000300090'
      });

      expect(edges[0]).to.deep.equal({
        from: 'http://bag.kadaster.nl/nummeraanduiding/0957200000300090',
        to: 'http://bag.kadaster.nl/openbareruimte/0957300000174823',
        type: 'hg:related'
      });

      done();
    });
  });
});
