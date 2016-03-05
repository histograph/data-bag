'use strict';
var Promise = require('bluebird');
var xml2js = require('xml2js');
var fs = require('fs');
var sax         = require('sax');
var saxpath = require('saxpath');

var GJV = require('geojson-validation');
var proj4 = require('proj4');
var reproject = require('reproject');
var jsts = require('jsts');
var reader = new jsts.io.GeoJSONReader();
var projDefs    = {
  'EPSG:2400': '+lon_0=15.808277777799999 +lat_0=0.0 +k=1.0 +x_0=1500000.0 +y_0=0.0 +proj=tmerc +ellps=bessel +units=m +towgs84=414.1,41.3,603.1,-0.855,2.141,-7.023,0 +no_defs',
  'EPSG:3006': '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs',
  'EPSG:4326': '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs',
  'EPSG:3857': '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs',
  'EPSG:28992': '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.237,50.0087,465.658,-0.406857,0.350733,-1.87035,4.0812 +units=m +no_defs'
};

module.exports = {
  title: 'BAG',
  url: 'http://bag.kadaster.nl',
  extractAddressesFromFile: extractAddressesFromFile
};

function extractAddressesFromFile(inputFileName, callback) {
  console.log(`Processing ${inputFileName}`);
  var addresses = [];
  var parser = new xml2js.Parser();
  var strict = true;

  var saxStream = sax.createStream(strict);
  fs.createReadStream(inputFileName, { encoding: 'utf8' })
    .pipe(saxStream);

  var streamer   = new saxpath.SaXPath(saxStream, '//bag_LVC:Nummeraanduiding');

  streamer.on('match', xml => {
    parser.parseString(xml, (err, result) => {
      if (err) {
        console.error(`Error parsing xml element ${xml} \n ${err.stack}`);
        return callback(err);
      }

      addresses.push({
        uri: module.exports.url + '/nummeraanduiding/' + result['bag_LVC:Nummeraanduiding']['bag_LVC:identificatie'][0],
        id: result['bag_LVC:Nummeraanduiding']['bag_LVC:identificatie'][0],
        huisnummer: result['bag_LVC:Nummeraanduiding']['bag_LVC:huisnummer'][0],
        postcode: result['bag_LVC:Nummeraanduiding']['bag_LVC:postcode'][0],
        startDate: result['bag_LVC:Nummeraanduiding']['bag_LVC:tijdvakgeldigheid']['bag:begindatumTijdvakGeldigheid'][0],
        endDate: result['bag_LVC:Nummeraanduiding']['bag_LVC:tijdvakgeldigheid']['bag:einddatumTijdvakGeldigheid'][0] ?
          result['bag_LVC:Nummeraanduiding']['bag_LVC:tijdvakgeldigheid']['bag:einddatumTijdvakGeldigheid'][0] : null
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
    console.log(`Returning ${addresses.length} buildings from ${inputFileName}`);
    return callback(null, addresses);
  });

}
