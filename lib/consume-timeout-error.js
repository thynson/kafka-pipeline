class ConsumeTimeoutError extends Error {

  constructor(consumedMessage, time) {
    super(`Message is not able to be consumed within ${time} millisecons`);
    Error.captureStackTrace(this);
    this.consumedMessage = consumedMessage;
  }

  get name() {
    return ConsumeTimeoutErrorth.name;
  }
}

module.exports = {ConsumeTimeoutError};
