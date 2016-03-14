'use strict';
var debug = require('debug')('file writer');
var fs = require('fs');
var Promise = require('bluebird');
var highland = require('highland');

module.exports.write = write;

function write(nodes, edges, outputPITsFile, outputRelationsFile){
  return new Promise((resolve, reject) => {
    if (!nodes) return reject(new Error('Empty nodes object'));
    if (!outputPITsFile) return reject(new Error('Requires an outputPITsFile to write to'));
    if (edges && !outputRelationsFile) return reject(new Error('Requires an outputRelationsFile to write to if edges are supplied'));

    debug(`Writing ${nodes.length} PITs`);
    if (edges) debug(`and ${edges.length} relations`);
    var nodeStream = highland(nodes);
    nodeStream.each(node => fs.appendFileSync(outputPITsFile, JSON.stringify(node) + '\n'));
    nodeStream.done(() => {
      if (!edges) return resolve(true);

      var edgeStream = highland(edges);
      edgeStream.each(edge => fs.appendFileSync(outputRelationsFile, JSON.stringify(edge) + '\n'));
      edgeStream.done(() => resolve(true));
    });
  });
}
