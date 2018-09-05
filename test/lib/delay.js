'use strict';

module.exports = async function delay(ms) {
  return new Promise((done)=> {
    setTimeout(done, ms);
    jest.advanceTimersByTime(ms);
    jest.runOnlyPendingTimers();
  })
}

