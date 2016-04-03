var fs = require('fs');
var path = require('path');

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
var should = chai.should();
var expect = chai.expect;

var geometryTools = require('../../helpers/geometrytools.js');

describe('geometry checking functions', function () {
  it('should invalidate an invalid feature', () => {
    var invalidFeature = {
      type: 'Feature',
      properties: { name: 'My non-simple hourglass-shaped geometry' },
      geometry: {
        type: 'Polygon',
        coordinates: [
          [
            [5.6, 52.4],
            [6.3, 52.9],
            [6.8, 52.1],
            [7.2, 52.6],
            [5.6, 52.4]
          ]
        ]
      }
    };

    return geometryTools.validateCoords(invalidFeature.geometry.coordinates, invalidFeature.geometry.type)
      .then(valid => expect(valid).to.be.false)
      .catch(errs => {
        console.error('Validation errors:', errs);
        return expect(errs).to.be.not.null;
      });
  });

  it('should reproject the coordinates to WGS84', () => {
    var geojson = {
      uri: 'http://bag.kadaster.nl/pand/0362100100084298',
      id: '0362100100084298',
      bouwjaar: '2011',
      geometry: {
        type: 'Polygon',
        coordinates: [
          [
            [
              117283.951,
              475941.101
            ],
            [
              117284.742,
              475944.408
            ],
            [
              117280.949,
              475945.315
            ],
            [
              117280.344,
              475942.787
            ],
            [
              117283.951,
              475941.101
            ]
          ]
        ]
      }
    };

    expect(geometryTools.toWGS84(geojson.geometry.coordinates[0][0])).to.deep.equal([4.834646702778442, 52.27019375226181]);

  });

  it('should join a gml-extracted position list to a WGS84 geojson-compatible one', () => {
    var testPosList = '116938.595 477068.148 0.0 ' +
      '116930.644 477071.854 0.0 ' +
      '116928.365 477066.959 0.0 ' +
      '116936.316 477063.253 0.0 ' +
      '116936.327 477063.277 0.0 ' +
      '116938.595 477068.148 0.0';

    return geometryTools.joinGMLposlist(testPosList, 'Polygon')
      .then(geojsoncoords => {
        console.log(JSON.stringify(geojsoncoords, null, 2));
        return geometryTools.validateCoords(geojsoncoords, 'Polygon')
          .then(valid => expect(valid).to.be.true)
          .catch(err => {
            console.log('geometry validation error:', err.stack);
            return expect(err).to.be.null;
          });
      });

  });

});
