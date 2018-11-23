'use strict';
jest.useFakeTimers();

import ConsumeStream from '../lib/consume-stream';
import Bluebird  from 'bluebird';
import fakeSleep from './lib/fake-sleep';

function createConsumer(option = {}) {
  return new ConsumeStream(Object.assign({}, {
    consumeTimeout: 5000,
    groupId: 'test',
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

describe('ConsumeStream', () => {


  test('Close immediately', (callback) => {
    const stream = createConsumer();
    stream.on('end', () => {
      callback();
    }).resume().end();
  });

  test('concurrency = 1', async () => {
    const messageConsumer = jest.fn().mockImplementation(() => {
      return new Promise((done) => {
        setTimeout(() => {
          done();
        }, 10);
      });
    });
    const stream = createConsumer({messageConsumer, consumeConcurrency: 1});
    expect(stream.write(createMessage({offset: 1}))).toBeTruthy();
    expect(stream.write(createMessage({offset: 2}))).toBeFalsy();
    expect(messageConsumer).toHaveBeenCalledTimes(1);
    await fakeSleep(10);
    expect(messageConsumer).toHaveBeenCalledTimes(2);
    await fakeSleep(10);
    await new Promise((done) => {
      stream.on('end', () => {
        done();
      }).resume().end();
    });
  });

  test('output order should be same with input order', async () => {
    const message1 = createMessage({offset: 1});
    const message2 = createMessage({offset: 2});
    const message3 = createMessage({offset: 3});
    const messageConsumer = jest.fn().mockImplementation((message) => {
      return new Promise((done) => setTimeout(done, 5 - message.offset));
    });
    const stream = createConsumer({messageConsumer, consumeConcurrency: 2});
    const onDataSpy = jest.fn();
    stream.on('data', onDataSpy);
    // Ensure first two message be "transformed"
    await Bluebird.fromCallback((done) => stream.write(message1, done));
    await Bluebird.fromCallback((done) => stream.write(message2, done));
    // Don't await the third message, as it won't be actually transformed
    // until one of first two have been consumed, as consumeConcurrency is 2
    stream.write(message3);

    // expect(messageConsumer).toHaveBeenCalledTimes(2);
    await fakeSleep(3);
    expect(onDataSpy).toHaveBeenCalledTimes(0);
    await fakeSleep(1);
    // message1 takes 4ms to consume
    expect(onDataSpy).toHaveBeenNthCalledWith(1, message1);
    // message2 takes 3ms to consume, so already be consumed
    expect(onDataSpy).toHaveBeenNthCalledWith(2, message2);
    expect(onDataSpy).toHaveBeenCalledTimes(2);
    await fakeSleep(1);
    expect(onDataSpy).toHaveBeenNthCalledWith(3, message3);
    await Bluebird.fromCallback((callback) => {
      stream.on('end', () => {
        callback();
      });
      stream.end();
    });
  });

  test('consume timeout', async () => {
    const messageConsumer = () => {
      return new Promise(() => null);
    };
    const failedMessageConsumer = jest.fn().mockImplementation(() => {
      return new Promise((done) => setImmediate(() => done()));
    });
    const stream = createConsumer({messageConsumer, failedMessageConsumer, consumeConcurrency: 2, consumeTimeout: 10});

    await Bluebird.fromCallback((done) => stream.write(createMessage({offset: 1}), done));
    await Bluebird.fromCallback((done) => stream.write(createMessage({offset: 2}), done));
    stream.end(createMessage({offset: 3}));
    await fakeSleep(10);
    expect(failedMessageConsumer).toHaveBeenCalledTimes(2);
    await fakeSleep(10);
    expect(failedMessageConsumer).toHaveBeenCalledTimes(3);
  });

  test('consume error without error collector', async () => {
    const messageConsumer = jest.fn().mockImplementation(({offset}) => {
      return new Promise((done, fail) => {
        setTimeout(() => {
          if (offset === 2) {
            fail(new Error());
          }
          done();
        }, offset);
      });
    });
    const stream = createConsumer({
      messageConsumer,
      failedMessageConsumer: null,
      consumeConcurrency: 2
    });

    stream.on('error', jest.fn());
    await Bluebird.fromCallback((done) => stream.write(createMessage({offset: 1}), done));

    await Bluebird.fromCallback((done) => stream.write(createMessage({offset: 2}), done));
    expect(messageConsumer).toHaveBeenCalledTimes(2);
    await fakeSleep(2);
    await Bluebird.fromCallback((done)=> stream.write(createMessage({offset: 3}), done));
    expect(messageConsumer).toHaveBeenCalledTimes(2);
  });
  test('consume error when flush without error collector', async () => {
    const messageConsumer = jest.fn().mockImplementation(({offset}) => {
      return new Promise((done, fail) => {
        setTimeout(() => {
          if (offset === 2) {
            fail(new Error());
          }
          done();
        }, offset);
      });
    });
    const stream = createConsumer({
      messageConsumer,
      failedMessageConsumer: null,
      consumeConcurrency: 1
    });

    await Bluebird.fromCallback((done) => stream.write(createMessage({offset: 1}), done));

    stream.end(createMessage({offset: 2}));
    expect(messageConsumer).toHaveBeenCalledTimes(1);
    await fakeSleep(1);
    expect(messageConsumer).toHaveBeenCalledTimes(2);
    await new Promise((done)=> {
      stream.on('error', async ()=> {
        setImmediate(done);
      });
      jest.runAllTimers();
    });
    await fakeSleep(100);
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
    stream.write(message1);
    stream.write(message2);
    stream.end(message3);
  });
});
