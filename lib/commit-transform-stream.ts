import {Transform} from 'stream';
import Bluebird from 'bluebird';

namespace CommitTransformStream {
  interface CommitFunction {
    (commits: {topic: String, partition: Number, offset: Number }[]): Promise<unknown> | unknown
  }
  export interface Option {
    commitFunction: CommitFunction
    commitInterval: number

  }
}


/**
 * @callback CommitFunction
 * @param commits {Array.<{topic: String, partition: Number, offset: Number }>}
 * @returns {Promise|*}
 */

/**
 * @private
 */
class CommitTransformStream extends Transform {

  private _bufferedOffset: Map<string, Map<number, number>> = new Map();
  private _options: CommitTransformStream.Option;
  private _forceCommitTimeout?: NodeJS.Timer;
  private _currentCommitPromise: Promise<any> = Bluebird.resolve();
  private _isDestroyed: boolean = false;

  /**
   * @param options {Object}
   * @param options.commitFunction {CommitFunction}
   * @param options.commitInterval {Number} A positive integer that specifies a minimal duration (in milliseconds)
   * between two offset commit request
   */
  constructor(options: CommitTransformStream.Option) {
    super({objectMode: true});
    this._options = options;
  }

  _popBufferedOffset() {
    const messages = this._bufferedOffset;
    this._bufferedOffset = new Map();
    const offsets = [];
    for (const [topic, partitions] of messages) {
      for (const [partition, offset] of partitions) {
        offsets.push({
            topic, offset, partition
        });
      }
    }
    return offsets;
  }

  _performCommit() {
    if (this._isDestroyed) {
      // Won't perform commit, return the rejected promise
      return this._currentCommitPromise;
    }
    if (this._forceCommitTimeout) {
      clearTimeout(this._forceCommitTimeout);
      this._forceCommitTimeout = null;
    }
    const originCommitPromise = this._currentCommitPromise;
    // We have to keep two commit operation from overlapped to each other,
    // otherwise it would trigger a bug of kafka-node that the callback of
    // former commit operation will be override by the latter and never be
    // called
    this._currentCommitPromise = originCommitPromise.then(() => {
      const offsets = this._popBufferedOffset();
      if (offsets.length === 0) {
        return null;
      }
      return Bluebird.resolve(this._options.commitFunction(offsets))
    });
    return this._currentCommitPromise;
  }

  _setForceCommitTimeout() {
    this._forceCommitTimeout = setTimeout(() => {
      this._performCommit()
        .catch((e) => {
          this._internalDestroy(e);
        });
    }, this._options.commitInterval);
  }

  _transform(message, unused, callback) {
    const {topic, partition, offset} = message;

    if (!this._bufferedOffset.has(topic)) {
      const partitions = new Map<number, number>();
      partitions.set(partition, offset + 1);
      this._bufferedOffset.set(topic, partitions);
    } else {
      this._bufferedOffset.get(topic).set(partition, offset + 1);
    }

    if (!this._forceCommitTimeout) {
      this._setForceCommitTimeout();
    }
    callback();
  }

  _flush(callback) {
    this._performCommit()
      .then(() => callback(), (e) => {
        this._internalDestroy(e);
        callback(e);
      });
  }

  _internalDestroy(e) {
    if (!this._isDestroyed) {
      this._isDestroyed = true;
      this.emit('error', e);
    }
  }
}


export default CommitTransformStream;
