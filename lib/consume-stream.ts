import Bluebird from 'bluebird';
import {Transform} from 'stream';
import ConsumeTimeoutError from './consume-timeout-error';
import {Message} from 'kafka-node';


export interface MessageConsumer {

  /**
   * @param message - Message to be consumed
   * @returns This function need to return a promise if the message is consumed asynchronously,
   * value of any other types indicates that the message have already been consumed.
   */
  (message: Message): Promise<unknown> | unknown;
}

export interface FailedMessageConsumer {
  /**
   * @param error Error
   * @param message {Object}
   * @param message.topic {String}
   * @param message.offset {Number}
   * @param message.values {String|Buffer}
   * @param message.partition {Number}
   * @returns {Promise|*} This function need to return a promise if the message is consumed asynchronously,
   * value of any other types indicates that the message have already been consumed.
   */
  (error: Error, message: Message): Promise<unknown> | unknown;
}

/**
 * ConsumeOption
 */
export interface ConsumeOption {
  consumeConcurrency: number,
  consumeTimeout: number,
  groupId: string

  messageConsumer: MessageConsumer

  failedMessageConsumer?: FailedMessageConsumer

}

class ConsumeStream extends Transform {

  private _options: ConsumeOption;
  private _currentConsumeConcurrency: number = 0;
  private _concurrentPromise: Promise<unknown> = Bluebird.resolve();
  private _waitingQueue: { message: unknown, done(e?: Error) }[] = [];
  private _lastMessageQueuedPromise: Promise<unknown> = Bluebird.resolve();
  private _isDestroyed: boolean = false;
  private _unhandledException?: Error;


  /**
   * @param options - Option controls the behaviors that how the message is consumed
   */
  constructor(options: ConsumeOption) {
    super({
      objectMode: true,
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

  private _consumeMessage(message) {
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
            fail(new ConsumeTimeoutError(message, this._options.consumeTimeout, this._options.groupId));
          }, this._options.consumeTimeout);
        })
      ])
      .catch((exception) => {
        if (typeof this._options.failedMessageConsumer !== 'function') {
          throw exception;
        }
        return Bluebird.resolve(this._options.failedMessageConsumer(exception, message));
      })
      .then(() => {
        this._dequeue();
        // Suppress Bluebird dangling promise warning, as we had tracked it manually for
        // controlling concurrency
        return null;
      });
  }

  private _concurrentConsumeMessage(message) {
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
          this.push(message);
        }
      }, (unhandledError) => {
        this._internalDestroy(unhandledError);
      });
  }

  private _enqueue(message) {
    return new Bluebird((done) => {
      ++this._currentConsumeConcurrency;
      if (this._currentConsumeConcurrency > this._options.consumeConcurrency) {
        return this._waitingQueue.push({message, done});
      }
      this._concurrentConsumeMessage(message);
      return done();
    });
  }

  private _dequeue() {
    this._currentConsumeConcurrency--;
    if (this._waitingQueue.length > 0) {
      const {message, done} = this._waitingQueue.shift();
      this._concurrentConsumeMessage(message);
      done();
    }
  }

  private _internalDestroy(e: Error) {
    this._isDestroyed = true;
    this.emit('error', e);
    this._unhandledException = e;
  }

}

export default ConsumeStream;

