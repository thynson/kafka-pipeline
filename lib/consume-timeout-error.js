'use strict';
const util = require('util');

/**
 * Error indicates that your messageConsumer did not finish handling message
 * within a given
 */
class ConsumeTimeoutError extends Error {

  /**
   * @param timedOutMessage {String|Buffer}
   * @param timeout {Number}
   * @param groupId {String}
   */
  constructor(timedOutMessage, timeout, groupId) {
    super(`Message consuming did not complete within ${timeout}`
      + `milliseconds when consuming message:\n${util.inspect(timedOutMessage)}`);
    Error.captureStackTrace(this, ConsumeTimeoutError);
    this.timedOutMessage = timedOutMessage;
    this.groupId = groupId;
  }

  get name() {
    return ConsumeTimeoutError.name;
  }
}

module.exports = {ConsumeTimeoutError};
