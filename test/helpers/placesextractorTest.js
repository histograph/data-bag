'use strict';
const fs = require('fs');
const path = require('path');

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const expect = chai.expect;

const buildingsExtractor = require('../../helpers/placesextractor.js');
const sourceFile = path.join(__dirname, '..', 'mockups', 'bag-WPL-snippet.xml');
const outputPITsFile = path.join(__dirname, '..', 'woonplaats.pits.ndjson');
const outputRelationsFile = path.join(__dirname, '..', 'woonplaats.relations.ndjson');

describe('places extraction', function () {
  after('Cleanup', () => {
    fs.unlinkSync(outputPITsFile);
  });

  it('should extract the places entries from a file', done => {
    buildingsExtractor.extractFromFile(sourceFile, outputPITsFile, outputRelationsFile, (err, result) => {
      if (err) throw err;

      const nodes = fs.readFileSync(outputPITsFile, 'utf-8')
        .split('\n')
        .filter(node => (node))
        .map(node => JSON.parse(node));

      console.log('result length:', nodes.length, '\n');
      console.log('extractedBuildingsFile number 1:', JSON.stringify(nodes[0], null, 2), '\n');

      const leeuwarden = nodes
        .filter(node => node.label === 'Leeuwarden');

      expect(leeuwarden[0]).to.deep.equal({
        uri: 'http://bag.kadaster.nl/woonplaats/1197',
        id: '1197',
        label: 'Leeuwarden',
        startDate: '2007110700000200',
        endDate: '2012010100000400',
        geometry: {
          coordinates: [
            [
              [
                [
                  5.8093667,
                  53.217454
                ],
                [
                  5.8090766,
                  53.2174305
                ],
                [
                  5.8089482,
                  53.2174187
                ],
                [
                  5.8093667,
                  53.217454
                ]
              ],
              [
                [
                  5.8095226,
                  53.1620268
                ],
                [
                  5.811024,
                  53.1623452
                ],
                [
                  5.8137097,
                  53.1629642
                ],
                [
                  5.8095226,
                  53.1620268
                ]
              ]
            ]
          ],
          type: 'MultiPolygon'
        }
      });
      done();
    });
  });
});
