'use strict';
const xml2js = require('xml2js');
const fs = require('fs');
const sax = require('sax');
const saxpath = require('saxpath');
const geometryTools = require('./geometrytools.js');
const writer = require('./bagwriter.js');

function extractFromFile(inputFileName, outputPITsFile, outputRelationsFile, callback) {
  console.log(`Processing ${inputFileName}`);
  const nodes = [];
  const edges = [];
  const parser = new xml2js.Parser();
  const strict = true;

  const saxStream = sax.createStream(strict);
  fs.createReadStream(inputFileName, { encoding: 'utf8' })
    .pipe(saxStream);

  const streamer = new saxpath.SaXPath(saxStream, '//bag_LVC:Woonplaats');

  streamer.on('match', xml => {
    parser.parseString(xml, (err, result) => {
      if (err) {
        console.error(`Error parsing xml element ${xml} \n ${err.stack}`);
        return callback(err);
      }

      const place = {
        uri: module.exports.url + result['bag_LVC:Woonplaats']['bag_LVC:identificatie'][0],
        id: result['bag_LVC:Woonplaats']['bag_LVC:identificatie'][0],
        label: result['bag_LVC:Woonplaats']['bag_LVC:woonplaatsNaam'][0],
        startDate: result['bag_LVC:Woonplaats']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:begindatumTijdvakGeldigheid'] ?
          result['bag_LVC:Woonplaats']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:begindatumTijdvakGeldigheid'][0] : null,
        endDate: result['bag_LVC:Woonplaats']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:einddatumTijdvakGeldigheid'] ?
          result['bag_LVC:Woonplaats']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:einddatumTijdvakGeldigheid'][0] : null
      };

      if (result['bag_LVC:Woonplaats']['bag_LVC:woonplaatsGeometrie'][0]['gml:Polygon']) {
        geometryTools.joinGMLposlist(
          result['bag_LVC:Woonplaats']['bag_LVC:woonplaatsGeometrie'][0]['gml:Polygon'][0]['gml:exterior'][0]['gml:LinearRing'][0]['gml:posList'][0]._,
          'polygon',
          2 // dimensions
        ).then(list => {
          const polygon = [];
          polygon[0] = list;

          place.geometry = {
            type: 'Polygon',
            coordinates: polygon
          };

          nodes.push(place);
        });
      } else if (result['bag_LVC:Woonplaats']['bag_LVC:woonplaatsGeometrie'][0]['gml:MultiSurface']) {
        const multiPolygon = [];
        multiPolygon[0] = [];

        result['bag_LVC:Woonplaats']['bag_LVC:woonplaatsGeometrie'][0]['gml:MultiSurface'][0]['gml:surfaceMember']
          .forEach(surfaceMember => {
            geometryTools.joinGMLposlist(
              surfaceMember['gml:Polygon'][0]['gml:exterior'][0]['gml:LinearRing'][0]['gml:posList'][0]._,
              'polygon',
              2 // dimensions
            )
              .then(list => multiPolygon[0].push(list))
              .catch(err => callback(err));
          });

        place.geometry = {
          type: 'MultiPolygon',
          coordinates: multiPolygon
        };

        nodes.push(place);
      }
    });
  });

  saxStream.on('error', err => {
    console.error(`saxStream threw error ${err.stack}`);

    // clear the error
    this._parser.error = null;
    this._parser.resume();
  });

  saxStream.on('end', () => writer.write(nodes, edges, outputPITsFile, outputRelationsFile)
    .then(result => callback(null, result))
    .catch(err => callback(err))
  );

}

module.exports = {
  title: 'BAG',
  url: 'http://bag.kadaster.nl/woonplaats/',
  extractFromFile
};
