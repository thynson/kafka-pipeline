'use strict';

import Bluebird from 'bluebird';
import {ConsumerGroup, ConsumerGroupOptions, Message} from 'kafka-node';
import CommitStream from './commit-stream';
import {default as ConsumeStream, ConsumeOption, MessageConsumer, FailedMessageConsumer} from './consume-stream';
import {EventEmitter} from 'events';

const debug = require('debug')('kafka-pipeline:ConsumerGroupPipeline');


const defaultOptions = {
  consumeTimeout: 5000,
  commitInterval: 10000,
  consumeConcurrency: 8
};

const defaultConsumerGroupOption = {
  fetchMaxBytes: 65536,
  sessionTimeout: 15000,
};

namespace ConsumerGroupPipeline {
  export interface Option {
    topic: string | string[];
    consumerGroupOption: ConsumerGroupOptions,
    messageConsumer: (message: Message) => Promise<unknown> | unknown;
    failedMessageConsumer?: (error: Error, message: Message) => Promise<unknown> | unknown;
    consumeTimeout?: number;
    consumeConcurrency?: number;
    commitInterval?: number;
  }
}

class ConsumerGroupPipeline extends EventEmitter {
  private _options: ConsumerGroupPipeline.Option;
  private _rebalanceCallback?: (e?: Error) => unknown;
  private _promiseOfOpening?: Promise<unknown>;
  private _promiseOfRunning?: Promise<unknown>;
  private _promiseOfClosing?: Promise<unknown>;
  private _consumeTransformStream?: ConsumeStream;
  private _consumer?: ConsumerGroup;

  private _topicMap: Map<string, ConsumeStream> = new Map();


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
  constructor(options: ConsumerGroupPipeline.Option) {
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
    this._promiseOfRunning = null; // Indicates whether this pipeline is running or closed
  }

  /**
   * Start a consume session, will be destroyed when rebalance or closed
   * @param consumerGroup
   * @param callback
   * @private
   */
  _pipelineSession(consumerGroup: ConsumerGroup, callback: (e?: Error) => unknown) {

    const commitFunction = (offsets) => {
      return new Bluebird((done, fail) => {
        if (offsets.length === 0) {
          return done();
        }
        return consumerGroup.sendOffsetCommitRequest(offsets.map((offset) => {
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
    const consumeTransformStream = new ConsumeStream({
      groupId: this._options.consumerGroupOption.groupId,
      consumeConcurrency: this._options.consumeConcurrency,
      messageConsumer: this._options.messageConsumer,
      failedMessageConsumer: this._options.failedMessageConsumer,
      consumeTimeout: this._options.consumeTimeout
    });
    const commitTransformStream = new CommitStream({
      commitFunction,
      commitInterval: this._options.commitInterval
    });
    const pumpQueuedMessage = () => {
      if (!this._promiseOfRunning ||
        this._rebalanceCallback ||
        consumeTransformStreamFull
      ) {
        return;
      }
      while (queuedMessages.length > 0) {
        if (!consumeTransformStream.write(queuedMessages.shift())) {
          consumeTransformStreamFull = true;
          return;
        }
      }
      consumerGroup.resume();
    };
    const onFetchCompleted = () => {
      consumerGroup.pause();
      pumpQueuedMessage();
    };
    const onMessage = (message) => {
      queuedMessages.push(message);
    };

    const cleanUpAndExit = (e?: Error) => {
      consumeTransformStream.removeAllListeners();
      commitTransformStream.removeAllListeners();
      // @ts-ignore
      consumerGroup.removeListener('message', onMessage);
      // @ts-ignore
      consumerGroup.removeListener('done', onFetchCompleted);
      // @ts-ignore
      consumerGroup.removeAllListeners('error');
      callback(e);
    };

    // @ts-ignore
    consumerGroup.once('error', (e) => {
      debug('Error occurred for consumer group: ', e);
      cleanUpAndExit(e);
    });

    consumeTransformStream.once('error', (e) => {
      debug('Error occurred for consumeTransformStream: ', e);
      cleanUpAndExit(e);
    });

    commitTransformStream.once('error', (e) => {
      debug('Error occurred for commitTransformStream: ', e);
      cleanUpAndExit(e);
    });

    commitTransformStream.once('end', () => {
      cleanUpAndExit(null);
    }).resume();


    consumerGroup.on('message', onMessage);
    // @ts-ignore
    consumerGroup.on('done', onFetchCompleted);

    consumeTransformStream.on('drain', () => {
      consumeTransformStreamFull = false;
      pumpQueuedMessage();
    });

    consumeTransformStream.pipe(commitTransformStream);
    this._consumeTransformStream = consumeTransformStream;
  }

  /**
   * Maintaining lifecycle
   * @param consumerGroup
   * @param callback {Function} A function will be called when consuming pipeline is closed
   * @private
   */
  _pipelineLifecycle(consumerGroup: ConsumerGroup, callback: (e?: Error) => unknown) {

    return this._pipelineSession(consumerGroup, (e) => {
      if (e) {
        return consumerGroup.close(() => {
          callback(e);
        });
      }

      if (this._rebalanceCallback) {
        const rebalanceCallback = this._rebalanceCallback;
        this._rebalanceCallback = null;

        // This pipeline is closed due to rebalance, we need to call `rebalanceCallback`
        // and restart a new pipeline and pass origin `callback` through

        this._pipelineLifecycle(consumerGroup, callback);
        return rebalanceCallback();
      }

      return consumerGroup.close(callback);
    });
  }


  /**
   * Start consuming message until close being called
   *
   * @param callback {Function} A function will be called when consuming pipeline is closed
   * @private
   */
  _consumeForever(callback: (e?: Error) => unknown) {

    const onRebalance = (isMember, rebalanceCallback) => {
      if (!(isMember && this._promiseOfRunning)) {
        rebalanceCallback();
        return;
      }
      this._rebalanceCallback = rebalanceCallback;
      // This function will finally triggered 'end' event of commit transform stream
      process.nextTick(() => this._consumeTransformStream.end());
    };

    const consumerGroupOptions = Object.assign(
      {},
      defaultConsumerGroupOption,
      this._options.consumerGroupOption,
      {
        onRebalance,
        autoCommit: false,
        paused: false,
        connectOnReady: true,
      }
    );

    this._consumer = new ConsumerGroup(consumerGroupOptions, this._options.topic);

    this._consumer.on('rebalanced', () => {
      this._consumer.resume();
    });

    this._pipelineLifecycle(this._consumer, callback);
  }


  /**
   *
   * @param topic
   * @param option {ConsumeOption}
   * @param option.messageConsumer {MessageConsumer} Function to consume message
   * @param [option.failedMessageConsumer] {FailedMessageConsumer} Function to consume message that failed to
   * consume by `messageConsumer`
   * @param [option.consumeTimeout=5000] {Number}
   * @param [option.consumeConcurrency=8] {Number}
   * @param [option.commitInterval=100000] {Number} Time in milliseconds between two commit, suggest to be greater than
   */
  subscribe(topic, option: ConsumeOption) {

  }

  unsubscribe(topic) {

  }

  /**
   * Start consuming
   */
  private async _doRun() {

    if (this._promiseOfRunning) {
      return this._promiseOfRunning;
    }
    this._promiseOfRunning = Bluebird
      .fromCallback((done) => {
        this._consumeForever((e) => {
          if (e) {
            return done(e);
          }
          return done();
        });
      })
      .catch((e) => {
        this.emit('error', e);
      })
      .finally(() => {
        this.emit('close');
      })
    ;
  }

  /**
   * Stop consuming
   * @returns {Promise}
   */
  private _doClose() {
    if (!this._promiseOfRunning) {
      return Bluebird.resolve();
    }
    const runningPromise = this._promiseOfRunning;
    this._promiseOfRunning = null;

    process.nextTick(() => this._consumeTransformStream.end());
    return runningPromise;
  }

  run(): Promise<unknown> {
    ConsumerGroupPipeline.checkOpened(this);
    ConsumerGroupPipeline.checkClosed(this);

    if (this._promiseOfRunning) {
      return this._promiseOfRunning;
    }

    this._promiseOfRunning = this._doRun();
    return this._promiseOfRunning;
  }

  private async _doOpen() {
    const onRebalance = (isMember, rebalanceCallback) => {
      if (!(isMember && this._promiseOfRunning)) {
        rebalanceCallback();
        return;
      }
      this._rebalanceCallback = rebalanceCallback;
      // This function will finally triggered 'end' event of commit transform stream
      process.nextTick(() => this._consumeTransformStream.end());
    };

    const consumerGroupOptions = Object.assign(
      {},
      defaultConsumerGroupOption,
      this._options.consumerGroupOption,
      {
        onRebalance,
        autoCommit: false,
        paused: true,
        connectOnReady: true,
      }
    );

    const consumerGroup = new ConsumerGroup(consumerGroupOptions, this._options.topic);


  }

  open(): Promise<unknown> {

    ConsumerGroupPipeline.checkClosed(this);
    if (this._promiseOfOpening) {
      return this._promiseOfOpening;
    }

    this._promiseOfOpening = this._doOpen();
    return this._promiseOfOpening;
  }

  close(): Promise<unknown> {
    ConsumerGroupPipeline.checkOpened(this);
    if (this._promiseOfClosing) {
      return this._promiseOfClosing;
    }
    this._promiseOfClosing = Promise.all([
      this._doClose(),
      this.run().catch(() => null)
    ]);
    return this._promiseOfClosing;
  }

  private static checkOpened(self: ConsumerGroupPipeline) {
    if (!self._promiseOfOpening) {
      throw new Error('This daemon is not opened yet');
    }
  }

  private static checkClosed(self: ConsumerGroupPipeline) {
    if (self._promiseOfClosing) {
      throw new Error('This daemon is closed');
    }
  }
}

module.exports = {ConsumerGroupPipeline};
