'use strict';
var fs = require('fs');
var path = require('path');

var nock = require('nock');

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
var expect = chai.expect;

var buildingsExtractor = require('../../helpers/buildingsextractor.js');

describe('buildings extraction', function() {
  it('should extract the building entries from a file', (done) => {
    var sourceFile = path.join(__dirname, '..', 'mockups', 'bag-PND-snippet.xml');
    var outputPITsFile = path.join(__dirname, '..', 'extract', 'pand.pits.ndjson');
    var outputRelationsFile = path.join(__dirname, '..', 'extract', 'pand.relations.ndjson');

    try {
      fs.unlinkSync(outputPITsFile);
    } catch(err) {}

    buildingsExtractor.extractFromFile(sourceFile, outputPITsFile, outputRelationsFile, (err, result) => {
      if (err) throw err;

      var nodes = fs.readFileSync(outputPITsFile, 'utf-8')
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