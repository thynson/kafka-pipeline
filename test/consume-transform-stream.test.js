'use strict';
jest.useFakeTimers();

const {ConsumeTransformStream} = require('../lib/consume-transform-stream');
const Bluebird = require('bluebird');
const {Transform} = require('stream');

function createConsumer(option) {
  return new ConsumeTransformStream(Object.assign({}, {
    consumeTimeout: 5000,
    consumeConcurrency: 8,
    messageConsumer: jest.fn().mockResolvedValue(null),
    failedMessageConsumer: jest.fn().mockResolvedValue(null)
  }, option));
}

const DEFAULT_TOPIC = 'test';
const DEFAULT_MESSAGE = 'message';
const DEFAULT_PARTITION = 0;

function createMessage(option) {
  return Object.assign({}, {
    message: DEFAULT_MESSAGE,
    topic: DEFAULT_TOPIC,
    partition: DEFAULT_PARTITION
  }, option);
}

describe('ConsumeTransformStream', () => {


  test('output order should be same with input order', async () => {
    const message1 = createMessage({offset: 1});
    const message2 = createMessage({offset: 2});
    const message3 = createMessage({offset: 3});
    const messageConsumer = (message) => {
      return new Promise((done) => setTimeout(done, 5 - message.offset));
    };
    const stream = createConsumer({messageConsumer, consumeConcurrency: 2});
    const onDataSpy = jest.fn();
    stream.on('data', onDataSpy);
    await Bluebird.fromCallback((done) => stream.write(message1, done));
    await Bluebird.fromCallback((done) => stream.write(message2, done));
    await Bluebird.fromCallback((done) => stream.write(message3, done));

    await new Bluebird((done) => {
      setTimeout(done, 3);
      jest.advanceTimersByTime(3);
      jest.runOnlyPendingTimers();
    });
    expect(onDataSpy).not.toHaveBeenCalled();
    await new Bluebird((done) => {
      setTimeout(done, 1);
      jest.advanceTimersByTime(1);
      jest.runOnlyPendingTimers();
    });
    expect(onDataSpy).toHaveBeenNthCalledWith(1, message1);
    expect(onDataSpy).toHaveBeenNthCalledWith(2, message2);
    await new Bluebird((done) => {
      setTimeout(done, 1);
      jest.advanceTimersByTime(1);
      jest.runOnlyPendingTimers();
    });
    expect(onDataSpy).toHaveBeenNthCalledWith(3, message3);
    await Bluebird.fromCallback((callback) => {
      stream.on('end', () => {
        callback();
      });
      stream.end();
    });
  });

  test('consume error without error collector', (callback) => {
    const message1 = createMessage({offset: 1});
    const messageConsumer = () => {
      return new Promise((done, fail) => setImmediate(() => fail(new Error())));
    };
    const stream = createConsumer({messageConsumer, failedMessageConsumer: null});

    stream.on('error', () => {
      callback();
    });
    stream.end(message1);
  });
  test('consume error with failed message consumer', (callback) => {
    const message1 = {offset: 1};
    const message2 = {offset: 2};
    const message3 = {offset: 3};
    const error1 = new Error('error1');
    const error2 = new Error('error2');
    const error3 = new Error('error3');
    const errors = [error1, error2, error3];
    const messageConsumer = () => {
      return new Promise((done, fail) => setImmediate(() => fail(errors.shift())));
    };
    const failedMessageConsumer = jest.fn().mockResolvedValue(null);
    const stream = createConsumer({messageConsumer, consumeConcurrency: 2, failedMessageConsumer});
    const onDataSpy = jest.fn();
    stream.on('data', onDataSpy);
    stream.write(message1);
    stream.write(message2);
    stream.end(message3);
    stream.on('end', () => {
      expect(failedMessageConsumer).toHaveBeenCalledTimes(3);
      expect(failedMessageConsumer).toHaveBeenNthCalledWith(1, error1, message1);
      expect(failedMessageConsumer).toHaveBeenNthCalledWith(2, error2, message2);
      expect(failedMessageConsumer).toHaveBeenNthCalledWith(3, error3, message3);
      expect(onDataSpy).toHaveBeenCalledTimes(3);
      expect(onDataSpy).toHaveBeenNthCalledWith(1, message1);
      expect(onDataSpy).toHaveBeenNthCalledWith(2, message2);
      expect(onDataSpy).toHaveBeenNthCalledWith(3, message3);
      callback();
    });
  });
});
