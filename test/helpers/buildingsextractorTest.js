'use strict';
const fs = require('fs');
const path = require('path');

const nock = require('nock');

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const expect = chai.expect;

const buildingsExtractor = require('../../helpers/buildingsextractor.js');
const sourceFile = path.join(__dirname, '..', 'mockups', 'bag-PND-snippet.xml');
const outputPITsFile = path.join(__dirname, '..', 'pand.pits.ndjson');
const outputRelationsFile = path.join(__dirname, '..', 'pand.relations.ndjson');

describe('buildings extraction', function() {
  after('Cleanup', () => {
    fs.unlinkSync(outputPITsFile);
  });
  
  it('should extract the building entries from a file', (done) => {
    buildingsExtractor.extractFromFile(sourceFile, outputPITsFile, outputRelationsFile, (err, result) => {
      if (err) throw err;

      const nodes = fs.readFileSync(outputPITsFile, 'utf-8')
        .split('\n')
        .filter(node => (node))
        .map(node => JSON.parse(node));

      console.log('result length:', nodes.length, '\n');
      console.log('extractedBuildingsFile number 19:', JSON.stringify(nodes[18], null, 2), '\n');

      expect(nodes[18]).to.deep.equal({
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