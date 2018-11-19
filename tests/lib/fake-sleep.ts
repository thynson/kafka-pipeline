'use strict';

export default async function delay(ms) {
  return new Promise((done) => {
    setTimeout(() => {
      jest.runAllImmediates();
      setImmediate(done);
    }, ms);
    jest.advanceTimersByTime(ms);
  });
};

