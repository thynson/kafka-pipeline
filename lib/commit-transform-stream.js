'use strict';
const {Transform} = require('stream');
const Bluebird = require('bluebird');

/**
 * @callback CommitFunction
 * @param commits {Array.<{topic: String, partition: Number, offset: Number }>}
 * @returns {Promise|*}
 */

/**
 * @private
 */
class CommitTransformStream extends Transform {

  /**
   * @param options {Object}
   * @param options.commitFunction {CommitFunction}
   * @param options.commitInterval {Number} A positive integer that specifies a minimal duration (in milliseconds)
   * between two offset commit request
   */
  constructor(options) {
    super({objectMode: true});
    this._options = options;
    this._bufferedOffset = new Map();
    this._forceCommitTimeout = null;
    this._currentCommitPromise = Bluebird.resolve(null);
    this._isDestroyed = false;
  }

  _popBufferedOffset() {
    const messages = this._bufferedOffset;
    this._bufferedOffset = new Map();
    const offsets = [];
    for (const [topic, partitions] of messages.entries()) {
      partitions.forEach((offset, partition) => {
        offsets.push({
          topic, offset, partition
        });
      });
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
      const partitions = [];
      partitions[partition] = offset + 1;
      this._bufferedOffset.set(topic, partitions);
    } else {
      this._bufferedOffset.get(topic)[partition] = offset + 1;
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

module.exports = {CommitTransformStream};

