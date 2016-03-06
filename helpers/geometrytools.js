'use strict';

var GJV = require('geojson-validation');
var proj4 = require('proj4');
var reader = new jsts.io.GeoJSONReader();

module.exports = {
  validateCoords: validateCoords,
  joinGMLposlist: joinGMLposlist,
  isValidGeoJSON: isValidGeoJSON,
  toWGS84: toWGS84
};

var proj4Defs    = {
  'EPSG:2400': '+lon_0=15.808277777799999 +lat_0=0.0 +k=1.0 +x_0=1500000.0 +y_0=0.0 +proj=tmerc +ellps=bessel +units=m +towgs84=414.1,41.3,603.1,-0.855,2.141,-7.023,0 +no_defs',
  'EPSG:3006': '+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs',
  'EPSG:4326': '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs',
  'EPSG:3857': '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs',
  'EPSG:28992': '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.237,50.0087,465.658,-0.406857,0.350733,-1.87035,4.0812 +units=m +no_defs'
};

function joinGMLposlist(posList, type) {
  return new Promise((resolve, reject) => {
    posList = posList.split(' ');

    var geojsonPosList = [];
    var counter = 0;

    while (posList.length !== 0) {
      var point = [];
      point.push(parseFloat(posList.shift()));
      point.push(parseFloat(posList.shift()));
      posList.shift(); // skip 3d height
      point = toWGS84(point)
        .map(coordinate => parseFloat(
          coordinate.toFixed(7))
        );
      geojsonPosList[counter] = point;

      counter += 1;
    }

    if (type === 'Polygon') {
      //add extra level of array
      var extraLevel = [[[]]];
      extraLevel[0] = geojsonPosList;
      resolve(extraLevel);
    }

    resolve(geojsonPosList);

  });
}

function validateCoords(geojsoncoords, type) {
  return new Promise((resolve, reject) => {
    var geojson = {
      type: 'Feature',
      geometry: {
        type: type,
        coordinates: geojsoncoords
      },
      properties: {}
    };

    GJV.isFeature(geojson, (valid, errs) => {
      if (!valid) {
        console.error('Validator rejecting geometry due to:', errs);
        reject(errs);
      } else {
        console.log('JSTS evaluated feature as valid: ' + isValidGeoJSON(geojson) + '\n');
        resolve(isValidGeoJSON(geojson));
      }
    });
  });
}

function isValidGeoJSON(geoJSONPolygon) {
  var jstsGeometry  = reader.read(geoJSONPolygon.geometry);

  if (jstsGeometry) {
    var validator = new jsts.operation.valid.IsValidOp(jstsGeometry);
    return validator.isValid();
  }
}

function toWGS84(point) {
  var EPSG28992 = '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.417,50.3319,465.552,-0.398957,0.343988,-1.8774,4.0725 +units=m +no_defs';

  return proj4(
    EPSG28992,
    proj4('WGS84')
  ).forward(point);
}