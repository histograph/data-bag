'use strict';
const xml2js = require('xml2js');
const fs = require('fs');
const sax = require('sax');
const saxpath = require('saxpath');
const highland = require('highland');
const writer = require('./bagwriter.js');

module.exports = {
  title: 'BAG',
  url: 'http://bag.kadaster.nl',
  extractFromFile: extractFromFile
};

function extractFromFile(inputFileName, outputPITsFile, outputRelationsFile, callback) {
  console.log(`Processing ${inputFileName}`);
  const nodes = [];
  const edges = [];
  const parser = new xml2js.Parser();
  const strict = true;

  const saxStream = sax.createStream(strict);
  fs.createReadStream(inputFileName, { encoding: 'utf8' })
    .pipe(saxStream);

  const streamer = new saxpath.SaXPath(saxStream, '//bag_LVC:Nummeraanduiding');

  streamer.on('match', xml => {
    parser.parseString(xml, (err, result) => {
      if (err) {
        console.error(`Error parsing xml element ${xml} \n ${err.stack}`);
        return callback(err);
      }

      nodes.push({
        uri: module.exports.url + '/nummeraanduiding/' + result['bag_LVC:Nummeraanduiding']['bag_LVC:identificatie'][0],
        id: result['bag_LVC:Nummeraanduiding']['bag_LVC:identificatie'][0],
        huisnummer: result['bag_LVC:Nummeraanduiding']['bag_LVC:huisnummer'] ?
          result['bag_LVC:Nummeraanduiding']['bag_LVC:huisnummer'][0] : null,
        huisletter: result['bag_LVC:Nummeraanduiding']['bag_LVC:huisletter'] ?
          result['bag_LVC:Nummeraanduiding']['bag_LVC:huisletter'] : null,
        postcode: result['bag_LVC:Nummeraanduiding']['bag_LVC:postcode'] ?
          result['bag_LVC:Nummeraanduiding']['bag_LVC:postcode'][0] : null,
        startDate: result['bag_LVC:Nummeraanduiding']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:begindatumTijdvakGeldigheid'] ?
          result['bag_LVC:Nummeraanduiding']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:begindatumTijdvakGeldigheid'][0] : null,
        endDate: result['bag_LVC:Nummeraanduiding']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:einddatumTijdvakGeldigheid'] ?
          result['bag_LVC:Nummeraanduiding']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:einddatumTijdvakGeldigheid'][0] : null
      });

      if (result['bag_LVC:Nummeraanduiding']['bag_LVC:gerelateerdeOpenbareRuimte']) {
        edges.push({
          from: module.exports.url + '/nummeraanduiding/' + result['bag_LVC:Nummeraanduiding']['bag_LVC:identificatie'][0],
          to: module.exports.url + '/openbareruimte/' + result['bag_LVC:Nummeraanduiding']['bag_LVC:gerelateerdeOpenbareRuimte'][0]['bag_LVC:identificatie'],
          type: 'hg:related'
        });
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
