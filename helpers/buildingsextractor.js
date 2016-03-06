'use strict';
var xml2js = require('xml2js');
var fs = require('fs');
var sax = require('sax');
var saxpath = require('saxpath');
var geometryTools = require('./geometrytools.js');

module.exports = {
  title: 'BAG',
  url: 'http://bag.kadaster.nl',
  extractFromFile: extractFromFile
};

function extractFromFile(inputFileName, callback) {
  console.log(`Processing ${inputFileName}`);
  var buildings = [];
  var parser = new xml2js.Parser();
  var strict = true;

  var saxStream = sax.createStream(strict);
  fs.createReadStream(inputFileName, { encoding: 'utf8' })
    .pipe(saxStream);

  var streamer   = new saxpath.SaXPath(saxStream, '//bag_LVC:Pand');

  streamer.on('match', xml => {
    parser.parseString(xml, (err, result) => {
      if (err) {
        console.error(`Error parsing xml element ${xml} \n ${err.stack}`);
        return callback(err);
      }

      geometryTools.joinGMLposlist(result['bag_LVC:Pand']['bag_LVC:pandGeometrie'][0]['gml:Polygon'][0]['gml:exterior'][0]['gml:LinearRing'][0]['gml:posList'][0]._)
        .then(list => {
          var polygon = [];
          polygon[0] = list;

          buildings.push({
            uri: module.exports.url + '/pand/' + result['bag_LVC:Pand']['bag_LVC:identificatie'][0],
            id: result['bag_LVC:Pand']['bag_LVC:identificatie'][0],
            bouwjaar: result['bag_LVC:Pand']['bag_LVC:bouwjaar'][0],
            startDate: result['bag_LVC:Pand']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:begindatumTijdvakGeldigheid'] ?
              result['bag_LVC:Pand']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:begindatumTijdvakGeldigheid'][0] : null,
            endDate: result['bag_LVC:Pand']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:einddatumTijdvakGeldigheid'] ?
              result['bag_LVC:Pand']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:einddatumTijdvakGeldigheid'][0] : null,
            geometry: {
              type: 'Polygon',
              coordinates: polygon
            }
          });
        });

    });
  });

  saxStream.on('error', err => {
    console.error(`saxStream threw error ${err.stack}`);

    // clear the error
    this._parser.error = null;
    this._parser.resume();
  });

  saxStream.on('end', () => {
    console.log(`Returning ${buildings.length} buildings from ${inputFileName}`);
    return callback(null, buildings);
  });

}
