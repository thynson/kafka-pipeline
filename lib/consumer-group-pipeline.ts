'use strict';

import Bluebird from 'bluebird';
import {ConsumerGroup, ConsumerGroupOptions} from 'kafka-node';
import CommitStream from './commit-stream';
import {default as ConsumeStream, FailedMessageConsumer, MessageConsumer} from './consume-stream';
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

export namespace ConsumerGroupPipeline {
  /**
   * Option for ConsumerGroupPipeline
   */
  export interface Option {
    /**
     * Topics of the messages to be consumed
     */
    topic: string | string[];

    /**
     * Options for the internal consumer group
     */
    consumerGroupOption: ConsumerGroupOptions,

    /**
     * Message consumer
     */
    messageConsumer: MessageConsumer;

    /**
     * Failed message consumer
     */
    failedMessageConsumer?: FailedMessageConsumer;

    /**
     * Timout when consuming each message
     */
    consumeTimeout?: number;

    /**
     * How many messages can be consumed simultaneously
     */
    consumeConcurrency?: number;

    /**
     * Time interval between two offset commit
     */
    commitInterval?: number;
  }
}

export class ConsumerGroupPipeline extends EventEmitter {
  private _options: ConsumerGroupPipeline.Option;
  private _rebalanceCallback?: (e?: Error) => unknown;
  private _consumeTransformStream?: ConsumeStream;
  private _consumer?: ConsumerGroup;
  private _consumingPromise?: Promise<unknown>;

  /**
   *
   * @param options Option of for the instance to be created
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
      if (!this._consumingPromise ||
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
   * Stop consuming
   * @returns {Promise}
   */
  private close() {
    if (!this._consumingPromise) {
      return Bluebird.resolve();
    }
    const runningPromise = this._consumingPromise;
    this._consumingPromise = null;

    process.nextTick(() => this._consumeTransformStream.end());
    return runningPromise;
  }


  /**
   * Start consuming message until being closed
   */
  private async start() {
    const onRebalance = (isMember, rebalanceCallback) => {
      if (!(isMember && this._consumingPromise)) {
        rebalanceCallback();
        return;
      }
      this._rebalanceCallback = rebalanceCallback;
      // This function will finally triggered 'end' event of commit transform stream
      process.nextTick(() => this._consumeTransformStream.end());
    };


    return new Bluebird((resolve, reject) => {
      this._consumer = new ConsumerGroup(Object.assign({},
        defaultConsumerGroupOption,
        this._options.consumerGroupOption,
        {
          onRebalance,
          autoCommit: false,
          paused: true,
          connectOnReady: true,
        }
      ), this._options.topic);

      this._consumer.on('rebalanced', () => {
        this._consumer.resume();
      });
      const onErrorBeforeConnect = (e) => {
        // @ts-ignore
        this._consumer.removeListener('error', resolve);
        reject(e);
      };
      this._consumer
        // @ts-ignore
        .once('error', onErrorBeforeConnect)
        .once('ready', () => {
          console.log('ready');
          // @ts-ignore
          this._consumer.removeListener('error', reject);
          resolve();
        });
      this._consumingPromise = Bluebird.fromCallback((done) => this._pipelineLifecycle(this._consumer, done));
    });
  }
}

export default ConsumerGroupPipeline;
