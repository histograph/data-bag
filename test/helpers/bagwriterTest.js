'use strict';
var fs = require('fs');
var writer = require('../../helpers/bagwriter.js');
var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
var expect = chai.expect;

describe('writer', function () {
  var testNodesFile = './test/extract/test.nodes';
  var testEdgesFile = './test/extract/test.edges';

  try {
    fs.unlinkSync(testNodesFile);
  } catch(err) {}

  it('should reject empty nodes object', () => {
    return expect(writer.write()).to.be.rejected;
  });

  it('should reject empty nodes object', () => {
    return writer.write([1,2,3], null, testNodesFile)
      .then(result => expect(result).to.equal(true));
  })

});
