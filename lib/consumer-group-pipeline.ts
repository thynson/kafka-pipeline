'use strict';

import {ConsumerGroup, ConsumerGroupOptions, Message} from 'kafka-node';
import CommitStream from './commit-stream';
import {default as ConsumeStream, FailedMessageConsumer, MessageConsumer} from './consume-stream';
import Bluebird from 'bluebird';
import EventEmitter = NodeJS.EventEmitter;


/**
 * @private
 */
const defaultOptions = {
  consumeTimeout: 5000,
  commitInterval: 10000,
  consumeConcurrency: 1
};

/**
 * @private
 */
const defaultConsumerGroupOption: Partial<ConsumerGroupOptions> = {
  fetchMaxBytes: 65536,
  sessionTimeout: 15000,
  heartbeatInterval: 5000
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

export class ConsumerGroupPipeline {
  private _options: ConsumerGroupPipeline.Option;
  private _rebalanceCallback?: (e?: Error) => unknown;
  private _consumer?: ConsumerGroup;
  private _consumingPromise?: Promise<unknown>;
  private _commitStream?: CommitStream;
  private _consumeStreamMap?: Map<number, ConsumeStream> = new Map();

  /**
   *
   * @param options Option of for the instance to be created
   */
  constructor(options: ConsumerGroupPipeline.Option) {

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
  private _pipelineSession(consumerGroup: ConsumerGroup, callback: (e?: Error) => unknown) {

    const queuedMessages = [];
    let partitionedQueuedMessage: Map<number, Message[]>;

    const partitionFulledMap = new Map<number, boolean>();

    const onFetchCompleted = () => {
      consumerGroup.pause();

      partitionedQueuedMessage = queuedMessages.reduce((result, message: Message) => {
        let currentPartitionQueue = result.get(message.partition);
        if (!currentPartitionQueue) {
          currentPartitionQueue = [];
          result.set(message.partition, currentPartitionQueue);
        }
        currentPartitionQueue.push(message);
        return result;
      }, new Map<number, Message[]>());
      partitionedQueuedMessage.forEach((messages, partition) => {
        pumpQueuedMessage(partition);
      });
    };

    const cleanUpAndExit = (e?: Error) => {
      for (const consumeStream of this._consumeStreamMap.values()) {
        consumeStream.removeAllListeners();
      }
      // @ts-ignore
      (consumerGroup as EventEmitter)
        .removeListener('message', onMessage)
        .removeListener('done', onFetchCompleted)
        .removeListener('error', cleanUpAndExit);
      this._commitStream.removeListener('error', cleanUpAndExit);
      callback(e);
    };
    this._commitStream.addListener('error', cleanUpAndExit);

    const onMessage = (message) => {
      queuedMessages.push(message);
    };

    const ensurePipelineForPartition = (partition: number): ConsumeStream => {
      let consumeStream = this._consumeStreamMap.get(partition);
      if (consumeStream) {
        return consumeStream;
      }
      consumeStream = new ConsumeStream({
        groupId: this._options.consumerGroupOption.groupId,
        consumeConcurrency: this._options.consumeConcurrency,
        messageConsumer: this._options.messageConsumer,
        failedMessageConsumer: this._options.failedMessageConsumer,
        consumeTimeout: this._options.consumeTimeout
      });

      consumeStream.once('error', (e) => {
        cleanUpAndExit(e);
      }).once('end', ()=>{
        this._consumeStreamMap.delete(partition);
        consumeStream.removeAllListeners();
        if (this._consumeStreamMap.size === 0) {
          cleanUpAndExit();
        }
      }).on('drain', () => {
        partitionFulledMap.set(partition, false);
        pumpQueuedMessage(partition);
      }).on('data', (message) => {
        this._commitStream.write(message);
      }).resume();
      this._consumeStreamMap.set(partition, consumeStream);
      partitionFulledMap.set(partition, false);
      return consumeStream;
    };

    const pumpQueuedMessage = (partition: number) => {
      if (!this._consumingPromise ||
        this._rebalanceCallback ||
        partitionFulledMap.get(partition)
      ) {
        return;
      }
      const queuedMessages = partitionedQueuedMessage.get(partition);
      while (queuedMessages.length > 0) {
        const message = queuedMessages.shift();
        const consumeStream = ensurePipelineForPartition(message.partition);
        if (!consumeStream.write(message)) {
          partitionFulledMap.set(partition, true);
          return;
        }
      }
      for (const [partition, queue] of partitionedQueuedMessage.entries()) {
        if (queue.length > 0) {
          return;
        }
      }
      consumerGroup.resume();

    };
    consumerGroup.on('error', cleanUpAndExit);
    consumerGroup.on('message', onMessage);
    // @ts-ignore
    consumerGroup.on('done', onFetchCompleted);
    consumerGroup.resume();
  }

  /**
   * Maintaining lifecycle
   * @param consumerGroup
   * @param callback {Function} A function will be called when consuming pipeline is closed
   * @private
   */
  private _pipelineLifecycle(consumerGroup: ConsumerGroup, callback: (e?: Error) => unknown) {

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
  public close(): Promise<unknown> {
    if (!this._consumingPromise) {
      return Promise.resolve();
    }
    const runningPromise = this._consumingPromise;
    this._consumingPromise = null;

    process.nextTick(() => {
      this._consumeStreamMap.forEach((consumeStream)=> consumeStream.end());
    });
    return runningPromise;
  }

  /**
   * Wait till the pipeline is closed
   */
  public async wait(): Promise<unknown> {
    if (this._consumingPromise) {
      return this._consumingPromise;
    }
    throw new Error('This pipeline is not started')
  }


  /**
   * Start consuming message until being closed
   */
  public async start(): Promise<unknown> {
    const onRebalance = (isMember, rebalanceCallback) => {
      if (!(isMember && this._consumingPromise)) {
        rebalanceCallback();
        return;
      }
      this._rebalanceCallback = rebalanceCallback;
      process.nextTick(() => {
        // This function will finally triggered 'end' event of commit transform stream
        this._consumeStreamMap.forEach((consumeStream)=> consumeStream.end());
      });
    };


    return new Promise((resolve, reject) => {
      const consumerGroup = this._consumer = new ConsumerGroup(Object.assign({},
        defaultConsumerGroupOption,
        this._options.consumerGroupOption,
        {
          onRebalance,
          autoCommit: false,
          paused: true,
          connectOnReady: true,
        }
      ), this._options.topic);

      const commitFunction = (offsets) => {
        return new Promise((done, fail) => {
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

      this._commitStream = new CommitStream({
        commitFunction,
        commitInterval: this._options.commitInterval
      }).resume();

      const onErrorBeforeConnect = (e) => {
        // @ts-ignore
        this._consumer.removeListener('error', onErrorBeforeConnect);
        reject(e);
      };
      // @ts-ignore
      (this._consumer as EventEmitter)
        .on('rebalanced', () => {
          this._consumer.resume();
        })
        .once('error', onErrorBeforeConnect)
        .once('connect', () => {
          // @ts-ignore
          this._consumer.removeListener('error', reject);
          resolve();
        });
      this._consumingPromise = Bluebird.fromCallback((cb) => {
        return this._pipelineLifecycle(this._consumer, cb);
      });
    });
  }
}

export default ConsumerGroupPipeline;
