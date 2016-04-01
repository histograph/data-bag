'use strict';
const fs = require('fs');
const writer = require('../../helpers/bagwriter.js');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const expect = chai.expect;

describe('writer', function () {
  const testNodesFile = './test/test.nodes';

  after('Cleanup', () => {
    fs.unlinkSync(testNodesFile);
  });

  it('should reject empty nodes object', () => {
    return expect(writer.write()).to.be.rejected;
  });

  it('should reject empty nodes object', () => {
    return writer.write([1,2,3], null, testNodesFile)
      .then(result => expect(result).to.equal(true));
  })

});
