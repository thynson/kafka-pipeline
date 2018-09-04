'use strict';

const Bluebird = require('bluebird');
const {ConsumerGroup} = require('kafka-node');
const {CommitTransformStream} = require('./commit-transform-stream');
const {ConsumeTransformStream} = require('./consume-transform-stream');
const {EventEmitter} = require('events');


const defaultOptions = {
  consumeTimeout: 5000,
  commitInterval: 10000,
  consumeConcurrency: 8
};


class ConsumerGroupPipeline extends EventEmitter {

  /**
   *
   * @param options.topic {String|String[]} A topic or a list of topics
   * @param options.consumerGroupOption {Object} Options passed to internal ConsumerGroup, see kafka-node documentation
   * @param options.consumerGroupOption.groupId {String} Group ID of this consumer
   * @param options.messageConsumer {MessageConsumerCallback} Function to consume message
   * @param [options.failedMessageConsumer] {FailedMessageConsumerCallback} Function to consume message that failed to
   * consume by `messageConsumer`
   * @param [options.consumeTimeout=5000] {Number}
   * @param [options.consumeConcurrency=8] {Number}
   * @param [options.commitInterval=100000] {Number} Time in milliseconds between two commit, suggest to be greater than
   * `consumeTimeout`.
   */
  constructor(options = {}) {
    super();

    if (typeof options !== 'object') {
      throw new TypeError('options is not an object');
    }
    if (typeof options.consumerGroupOption !== 'object') {
      throw new TypeError('missing consumerGroupOptions');
    }
    if (typeof options.consumerGroupOption.groupId !== 'string' || options.consumerGroupOption.groupId === '') {
      throw new TypeError('Invalid groupId');
    }
    this._options = Object.assign({}, defaultOptions, options);

    this._rebalanceCallback = null;
    this._runningPromise = null; // Indicates whether this pipeline is running or closed
  }

  /**
   * Start a consume session, will be destroyed when rebalance or closed
   * @param callback
   * @private
   */
  _pipelineSession(callback) {

    const commitFunction = (offsets) => {
      return new Promise((done, fail) => {
        if (offsets.length === 0) {
          return done();
        }
        return this._consumerGroup.sendOffsetCommitRequest(offsets.map((offset) => {
          return Object.assign({}, {metadata: 'm'}, offset, {metadata: 'm'});
        }), (commitError) => {
          if (commitError) {
            return fail(commitError);
          }
          return done();
        });
      });
    };
    let consumeTransformStreamFull = false;
    const queuedMessages = [];
    const consumeTransformStream = new ConsumeTransformStream({
      maxConsumeConcurrency: this._options.consumeConcurrency,
      messageConsumer: this._options.messageConsumer,
      failedMessageConsumer: this._options.failedMessageConsumer
    });
    const commitTransformStream = new CommitTransformStream({
      commitFunction,
      minCommitInterval: this._options.commitInterval
    });

    consumeTransformStream.once('error', (e) => {
      commitTransformStream.removeAllListeners();
      callback(e);
    });

    commitTransformStream.once('error', (e) => {
      consumeTransformStream.removeAllListeners();
      callback(e);
    });

    commitTransformStream.once('end', () => {
      callback();
    }).resume();

    const pumpQueueMessage = () => {
      if (!this._runningPromise || this._rebalanceCallback) {
        return;
      }
      if (consumeTransformStreamFull) {
        return;
      }
      while (queuedMessages.length > 0) {
        if (!consumeTransformStream.write(queuedMessages.shift())) {
          consumeTransformStreamFull = true;
          return;
        }
      }
      this._consumerGroup.resume();
    };

    this._consumerGroup.on('message', (message) => {
      this._consumerGroup.pause();
      queuedMessages.push(message);
      pumpQueueMessage();
    });

    consumeTransformStream.on('drain', () => {
      consumeTransformStreamFull = false;
      if (!this._runningPromise || this._rebalanceCallback) {
        return;
      }
      pumpQueueMessage();
    });

    consumeTransformStream.pipe(commitTransformStream);
    this._consumeTransformStream = consumeTransformStream;
  }

  /**
   * Maintaining lifecycle
   * @param callback {Function} A function will be called when consuming pipeline is closed
   * @private
   */
  _pipelineLifecycle(callback) {

    return this._pipelineSession((e) => {
      if (e) {
        if (this._runningPromise) {
          return this.close(true)
            .then(() => callback(e));
        }
        if (this._runningPromise) {
          // Error occurred during consuming
          this._runningPromise = null;
          this.emit('error', e);
          return this._consumerGroup.close(() => {
            return callback(e);
          });
        }
        return callback(e);
        // Error occurred during closing
      }

      if (this._rebalanceCallback) {
        const rebalanceCallback = this._rebalanceCallback;
        this._rebalanceCallback = null;

        // This pipeline is closed due to rebalance, we need to call `rebalanceCallback`
        // and restart a new pipeline and pass origin `callback` through

        this._pipelineLifecycle(callback);
        return rebalanceCallback();
      }

      return this._consumerGroup.close(callback);
    });
  }


  /**
   * Start consuming message until close being called
   *
   * @param callback {Function} A function will be called when consuming pipeline is closed
   * @private
   */
  _consumeForever(callback) {

    const onRebalance = (isMember, rebalanceCallback) => {
      if (!(isMember && this._runningPromise)) {
        rebalanceCallback();
        return;
      }
      this._rebalanceCallback = rebalanceCallback;
      // This function will finally triggered 'end' event of commit transform stream
      this._consumeTransformStream.end();
    };

    const consumerGroupOptions = Object.assign(
      {},
      this._options.consumerGroupOption,
      {
        onRebalance: (isMember, rebalanceCallback) => {
          if (!(isMember && this._runningPromise)) {
            rebalanceCallback();
            return;
          }
          this._rebalanceCallback = rebalanceCallback;
          // This function will finally triggered 'end' event of commit transform stream
          this._consumeTransformStream.end();
        },
        autoCommit: false,
      }
    );

    this._consumerGroup = new ConsumerGroup(consumerGroupOptions, [this._options.topic]);

    this._consumerGroup.on('rebalanced', () => {
      if (this._consumerGroup.paused) {
        this._consumerGroup.resume();
      }
    });

    this._consumerGroup.once('error', (e) => {
      this.close();
      callback(e);
    });

    this._pipelineLifecycle(callback);
  }

  /**
   * Start consuming
   */
  start() {

    if (this._runningPromise) {
      return;
    }
    this._runningPromise = Bluebird.fromCallback((done) => {
      this._consumeForever((e) => {
        if (e) {
          return done(e);
        }
        return done();
      });
    });
  }

  /**
   * Stop consuming
   * @returns {Promise}
   */
  close(force = false) {
    if (!this._runningPromise) {
      return Bluebird.resolve(null);
    }
    const listenPromise = this._runningPromise;
    this._runningPromise = null;

    if (!force) {
      this._consumeTransformStream.end();
      return listenPromise;
    }

    if (this._consumerGroup !== null) {
      const consumerGroup = this._consumerGroup;
      this._consumerGroup = true;
      return Bluebird.fromCallback((callback) => consumerGroup.close(true, () => {
        // Ignore error here
        callback()
      }));
    }
    return Bluebird.resolve(null);
  }
}

module.exports = {ConsumerGroupPipeline};
