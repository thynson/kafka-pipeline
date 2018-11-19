'use strict';

import CommitTransformStream from '../lib/commit-transform-stream';
import Bluebird  from 'bluebird';
import fakeSleep from './lib/fake-sleep';

jest.useFakeTimers();

describe('CommitTransformStream', () => {

  test('Closed without writing message', (callback) => {

    const commitInterval = 1;
    // jest.fn().mockRejectedValue() seems to be buggy
    const commitFunction = jest.fn();

    const cts = new CommitTransformStream({commitInterval, commitFunction});

    cts.on('end', () => {
      callback();
    }).resume().end();
  });

  test('Commit error', async () => {
    const commitInterval = 10;
    // jest.fn().mockRejectedValue() seems to be buggy
    const commitFunction = jest.fn().mockImplementation(() => {
      return new Promise((done, fail) => {
        setTimeout(() => {
          fail(new Error());
        }, 50);
      });
    });

    const cts = new CommitTransformStream({commitInterval, commitFunction});
    const onErrorSpy = jest.fn();
    cts.on('error', onErrorSpy);
    const transformSpy = jest.spyOn(cts, '_transform');
    await Bluebird.fromCallback((done) => cts.write({offset: 1, topic: 'test', partition: 0}, done));
    expect(transformSpy).toHaveBeenCalled();
    expect(onErrorSpy).not.toHaveBeenCalled();
    await fakeSleep(10);
    expect(commitFunction).toHaveBeenCalled();
    expect(onErrorSpy).not.toHaveBeenCalled();
    await fakeSleep(50);
    expect(commitFunction).toHaveBeenCalledTimes(1);
    await Bluebird.fromCallback((done) => cts.write({offset: 2, topic: 'test', partition: 1}, done));
    await Bluebird.fromCallback((done) => cts.write({offset: 2, topic: 'test', partition: 3}, done));

    await fakeSleep(50);
    expect(commitFunction).toHaveBeenCalledTimes(1);
    expect(onErrorSpy).toHaveBeenCalledTimes(1);
    await Bluebird.fromCallback((done) => cts.write({offset: 3, topic: 'test', partition: 2}, done));
    await fakeSleep(50);
    expect(onErrorSpy).toHaveBeenCalledTimes(1);
    expect(commitFunction).toHaveBeenCalledTimes(1);
  });

  test('Commit error on flush', (callback) => {
    const commitInterval = 10;
    // jest.fn().mockRejectedValue() seems to be buggy
    const commitFunction = jest.fn().mockImplementation(() => Bluebird.reject(new Error()));

    const cts = new CommitTransformStream({commitInterval, commitFunction});
    const onErrorSpy = jest.fn().mockImplementation(() => {
      callback();
    });
    cts.on('error', onErrorSpy);
    cts.write({offset: 1, topic: 'test', partition: 0});
    cts.end();
  });
  test('Commit normally', async () => {
    const commitInterval = 500;

    const commitFunction = jest.fn().mockResolvedValue(null);

    const cts = new CommitTransformStream({commitInterval, commitFunction});
    await Bluebird.fromCallback((done) => cts.write({offset: 1, topic: 'test', partition: 0}, done));
    await Bluebird.fromCallback((done) => cts.write({offset: 100, topic: 'test', partition: 1}, done));
    await fakeSleep(400);

    expect(commitFunction).not.toHaveBeenCalled();
    await fakeSleep(100);
    expect(commitFunction).toHaveBeenNthCalledWith(1, expect.arrayContaining([
      expect.objectContaining({
        offset: 2,
        topic: 'test',
        partition: 0,
      }), expect.objectContaining({
        offset: 101,
        topic: 'test',
        partition: 1,
      })
    ]));
    await Bluebird.fromCallback((done) => cts.write({offset: 2, topic: 'test', partition: 0}, done));
    await fakeSleep(600);
    expect(commitFunction).toHaveBeenNthCalledWith(2, [
      expect.objectContaining({
        offset: 3,
        topic: 'test',
        partition: 0,
      })
    ]);
  });

  test('Commit only performed after commitInterval', async () => {
    const commitInterval = 500;
    const commitFunction = jest.fn().mockResolvedValue(null);

    const cts = new CommitTransformStream({commitInterval, commitFunction});
    await Bluebird.fromCallback((done) => cts.write({offset: 1, topic: 'test', partition: 0}, done));
    await fakeSleep(499);
    expect(commitFunction).not.toHaveBeenCalled();
    await fakeSleep(1);
    expect(commitFunction).toHaveBeenNthCalledWith(1, expect.arrayContaining([
      expect.objectContaining({
        offset: 2,
        topic: 'test',
        partition: 0,
      })
    ]));
  });


  test('Commit should be performed when stream is finished', async () => {
    const commitInterval = 500;
    const commitFunction = jest.fn().mockResolvedValue(null);

    const cts = new CommitTransformStream({commitInterval, commitFunction});
    await Bluebird.fromCallback((done) => cts.write({offset: 1, topic: 'test', partition: 0}, done));
    await new Promise((done) => {
      setTimeout(done, 200);
      jest.advanceTimersByTime(200);
      jest.runOnlyPendingTimers();
    });
    expect(commitFunction).not.toHaveBeenCalled();
    await new Promise((done) => {
      cts.on('end', () => {
        expect(commitFunction).toHaveBeenCalledWith(expect.arrayContaining([
          expect.objectContaining({
            offset: 2,
            topic: 'test',
            partition: 0,
          })
        ]));
        done();
      }).resume();
      cts.end();
    });
  });

  test('Next commit should await previous commit', async () => {
    const commitInterval = 500;
    let commitCallback = null;
    const commitFunction = jest.fn().mockImplementation(() => {
      return new Promise((done) => {
        if (commitCallback) {
          done();
          return;
        }
        commitCallback = done;
      });
    });
    const cts = new CommitTransformStream({commitInterval, commitFunction});
    await Bluebird.fromCallback((done) => cts.write({offset: 1, topic: 'test', partition: 0}, done));
    await fakeSleep(500);
    expect(commitFunction).toHaveBeenCalledTimes(1);
    const promise = cts['_currentCommitPromise'];
    await Bluebird.fromCallback((done) => cts.write({offset: 2, topic: 'test', partition: 0}, done));
    await fakeSleep(500);
    expect(commitFunction).toHaveBeenCalledTimes(1);
    commitCallback();
    await promise;

    expect(commitFunction).toHaveBeenCalledTimes(2);
  });


});
