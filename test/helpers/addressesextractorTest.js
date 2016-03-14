'use strict';
var fs = require('fs');
var path = require('path');
var nock = require('nock');

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
var expect = chai.expect;

var addressesExtractor = require('../../helpers/addressesextractor.js');

describe('addresses extraction', () => {
  it('should extract an address from a mocked snippet', done => {
    var sourceFile = path.join(__dirname, '..', 'mockups', 'bag-NUM-snippet.xml');
    var outputPITsFile = path.join(__dirname, '..', 'extract', 'adres.pits.ndjson');
    var outputRelationsFile = path.join(__dirname, '..', 'extract', 'adres.relations.ndjson');

    try {
      fs.unlinkSync(outputPITsFile);
    } catch(err) {}

    try {
      fs.unlinkSync(outputRelationsFile);
    } catch(err) {}

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
