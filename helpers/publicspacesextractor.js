'use strict';
var xml2js = require('xml2js');
var fs = require('fs');
var sax = require('sax');
var saxpath = require('saxpath');

module.exports = {
  title: 'BAG',
  url: 'http://bag.kadaster.nl',
  extractFromFile: extractFromFile
};

function extractFromFile(inputFileName, callback) {
  console.log(`Processing ${inputFileName}`);
  var publicSpaceNodes = [];
  var publicSpaceEdges = [];
  var parser = new xml2js.Parser();
  var strict = true;

  var saxStream = sax.createStream(strict);
  fs.createReadStream(inputFileName, { encoding: 'utf8' })
    .pipe(saxStream);

  var streamer   = new saxpath.SaXPath(saxStream, '//bag_LVC:OpenbareRuimte');

  streamer.on('match', xml => {
    parser.parseString(xml, (err, result) => {
      if (err) {
        console.error(`Error parsing xml element ${xml} \n ${err.stack}`);
        return callback(err);
      }

      if (result['bag_LVC:OpenbareRuimte']['bag_LVC:openbareRuimteType'][0] === 'Weg') {
        publicSpaceNodes.push({
          uri: module.exports.url + '/openbareruimte/' + result['bag_LVC:OpenbareRuimte']['bag_LVC:identificatie'][0],
          id: result['bag_LVC:OpenbareRuimte']['bag_LVC:identificatie'][0],
          name: result['bag_LVC:OpenbareRuimte']['bag_LVC:openbareRuimteNaam'] ?
            result['bag_LVC:OpenbareRuimte']['bag_LVC:openbareRuimteNaam'][0] : null,
          startDate: result['bag_LVC:OpenbareRuimte']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:begindatumTijdvakGeldigheid'] ?
            result['bag_LVC:OpenbareRuimte']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:begindatumTijdvakGeldigheid'][0] : null,
          endDate: result['bag_LVC:OpenbareRuimte']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:einddatumTijdvakGeldigheid'] ?
            result['bag_LVC:OpenbareRuimte']['bag_LVC:tijdvakgeldigheid'][0]['bagtype:einddatumTijdvakGeldigheid'][0] : null
        });

        publicSpaceEdges.push({
          from: module.exports.url + '/openbareruimte/' + result['bag_LVC:OpenbareRuimte']['bag_LVC:identificatie'][0],
          to: module.exports.url + '/woonplaats/' + result['bag_LVC:OpenbareRuimte']['bag_LVC:gerelateerdeWoonplaats'][0]['bag_LVC:identificatie'],
          type: 'hg:liesIn'
        })
      }

    });
  });

  saxStream.on('error', err => {
    console.error(`saxStream threw error ${err.stack}`);

    // clear the error
    this._parser.error = null;
    this._parser.resume();
  });

  saxStream.on('end', () => {
    console.log(`Returning ${publicSpaceNodes.length} public space PITs and ${publicSpaceEdges.length} public space relations from ${inputFileName}`);
    return callback(null, publicSpaceNodes, publicSpaceEdges);
  });

}
