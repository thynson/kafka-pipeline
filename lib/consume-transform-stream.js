'use strict';
const Bluebird = require('bluebird');
const {Transform} = require('stream');

class ConsumeTimeoutError extends Error {

}

/**
 * @callback MessageConsumerCallback
 * @param message {Object}
 * @param message.topic {String}
 * @param message.offset {Number}
 * @param message.values {String|Buffer}
 * @param message.partition {Number}
 * @returns {Promise}
 */

/**
 * @callback FailedMessageConsumerCallback
 * @param error
 * @param message {Object}
 * @param message.topic {String}
 * @param message.offset {Number}
 * @param message.values {String|Buffer}
 * @param message.partition {Number}
 * @returns {Promise}
 *
 */

/**
 * @private
 */
class ConsumeTransformStream extends Transform {

  /**
   *
   * @param options.messageConsumer {MessageConsumerCallback}
   * @param options {Object}
   * @param options.consumeConcurrency  {Number}
   * @param options.consumeTimeout         {Number}
   * @param [options.failedMessageConsumer]  {FailedMessageConsumerCallback}
   */
  constructor(options) {
    super({
      objectMode: true,
      decodeString: true,
      highWaterMark: options.consumeConcurrency + 1
    });
    this._options = options;

    this._currentConsumeConcurrency = 0;
    this._concurrentPromise = Bluebird.resolve();
    this._waitingQueue = [];
    this._lastMessageQueuedPromise = Bluebird.resolve();
    this._isDestroyed = false;
    this._unhandledException = null;
  }


  _transform(message, encoding, callback) {
    this._lastMessageQueuedPromise = this._enqueue(message);
    this._lastMessageQueuedPromise.then(() => {
      callback(null);
    });
  }

  _flush(callback) {
    this._lastMessageQueuedPromise
      .then(() => {
        return this._concurrentPromise;
      })
      .then(() => {
        callback(this._unhandledException);
      });
  }

  _consumeMessage(message) {
    let timeoutHandler = null;
    let timeoutDone = null;
    return Bluebird
      .all([
        Bluebird
          .resolve(this._options.messageConsumer(message))
          .finally(() => {
            if (timeoutHandler !== null) {
              clearTimeout(timeoutHandler);
              timeoutDone();
            }
          }),
        new Bluebird((done, fail) => {
          timeoutDone = done;
          timeoutHandler = setTimeout(() => {
            timeoutDone = null;
            timeoutHandler = null;
            fail(new ConsumeTimeoutError());
          }, this._options.consumeTimeout);
        })
      ])
      .catch((exception) => {
        if (typeof this._options.failedMessageConsumer !== 'function') {
          throw exception;
        }
        return Bluebird.resolve(this._options.failedMessageConsumer(exception, message));
      });
  }

  _concurrentConsumeMessage(message) {
    if (this._isDestroyed) {
      return;
    }
    const concurrentPromise = this._concurrentPromise;
    this._concurrentPromise = Bluebird
      .all([
        concurrentPromise,
        this._consumeMessage(message)
      ]).then(() => {
        if (!this._isDestroyed) {
          this._dequeue();
          this.push(message);
        }
      }, (unhandledError) => {
        this._internalDestroy(unhandledError);
      });
  }

  _enqueue(message) {
    return new Bluebird((done) => {
      ++this._currentConsumeConcurrency;
      if (this._currentConsumeConcurrency > this._options.consumeConcurrency) {
        return this._waitingQueue.push({message, done});
      }
      this._concurrentConsumeMessage(message);
      return done();
    });
  }

  _dequeue() {
    this._currentConsumeConcurrency--;
    if (this._waitingQueue.length > 0) {
      const {message, done} = this._waitingQueue.shift();
      this._concurrentConsumeMessage(message);
      done();
    }
  }

  _internalDestroy(e) {
    this._isDestroyed = true;
    this.emit('error', e);
    this._unhandledException = e;
  }

}

module.exports = {ConsumeTransformStream};
