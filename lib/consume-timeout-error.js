'use strict';

/**
 * Error indicates that your messageConsumer did not finish handling message
 * within a given
 */
class ConsumeTimeoutError extends Error {

  constructor(timedOutMessage, time) {
    super(`Message consuming did not complete within ${time} milliseconds`);
    Error.captureStackTrace(this);
    this.timedOutMessage = timedOutMessage;
  }

  get name() {
    return ConsumeTimeoutError.name;
  }

  /**
   * Override for better logging
   * @returns {string}
   */
  valueOf() {
    return `{ ${this.stack}\n  timedOutMessage:\n    ${this.timedOutMessage.valueOf()}}\n`;
  }
}

module.exports = {ConsumeTimeoutError};
