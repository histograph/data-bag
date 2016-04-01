'use strict';
const fs = require('fs');
const path = require('path');
const nock = require('nock');

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const expect = chai.expect;

const publicSpacesExtractor = require('../../helpers/publicspacesextractor.js');
const sourceFile = path.join(__dirname, '..', 'mockups', 'bag-OPR-snippet.xml');
const outputPITsFile = path.join(__dirname, '..', 'openbareruimte.pits.ndjson');
const outputRelationsFile = path.join(__dirname, '..', 'openbareruimte.relations.ndjson');

describe('public spaces extraction', function() {
  after('Cleanup', () => {
    fs.unlinkSync(outputPITsFile);
    fs.unlinkSync(outputRelationsFile);
  });

  it('should extract the public spaces from the snippet', done => {
    return publicSpacesExtractor.extractFromFile(sourceFile,  outputPITsFile, outputRelationsFile, (err, result) => {
      if (err) return done(err);

      const nodes = fs.readFileSync(outputPITsFile, 'utf-8')
        .split('\n')
        .filter(node => (node))
        .map(node => JSON.parse(node));

      const edges = fs.readFileSync(outputRelationsFile, 'utf-8')
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
