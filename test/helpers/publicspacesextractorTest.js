'use strict';
var fs = require('fs');
var path = require('path');
var nock = require('nock');

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
var expect = chai.expect;

var publicSpacesExtractor = require('../../helpers/publicspacesextractor.js');

describe('public spaces extraction', function() {
  it('should extract the public spaces from the snippet', done => {
    var sourceFile = path.join(__dirname, '..', 'mockups', 'bag-OPR-snippet.xml');
    var outputPITsFile = path.join(__dirname, '..', 'extract', 'openbareruimte.pits.ndjson');
    var outputRelationsFile = path.join(__dirname, '..', 'extract', 'openbareruimte.relations.ndjson');

    publicSpacesExtractor.extractFromFile(sourceFile,  outputPITsFile, outputRelationsFile, (err, result) => {
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
      expect(nodes.length).to.equal(3);
      expect(nodes.length).to.equal(3);

      expect(nodes[0]).to.deep.equal({
        uri: 'http://bag.kadaster.nl/openbareruimte/0003300000116985',
        id: '0003300000116985',
        name: 'Abel Eppensstraat',
        startDate: '1956032800000000',
        endDate: null
      });

      expect(edges[0]).to.deep.equal({
        from: 'http://bag.kadaster.nl/openbareruimte/0003300000116985',
        to: 'http://bag.kadaster.nl/woonplaats/3386',
        type: 'hg:liesIn'
      });

      done();
    });
  });

});
