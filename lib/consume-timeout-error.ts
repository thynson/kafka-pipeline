'use strict';
import util from 'util';
import {Message} from "kafka-node";

/**
 * Error indicates that your messageConsumer did not finish handling message
 * within a given
 */
export default class ConsumeTimeoutError extends Error {

  /**
   * @param timedOutMessage - Message of for this timeout error
   * @param timeout - Configured timeout of the consumer
   * @param groupId - Configured group id of the consumer
   */
  constructor(private timedOutMessage : Message,
              private timeout: number,
              private groupId: string) {
    super(`Message consuming did not complete within ${timeout}`
      + `milliseconds when consuming message:\n${util.inspect(timedOutMessage)}`);
    Error.captureStackTrace(this, ConsumeTimeoutError);
  }

  get name() {
    return ConsumeTimeoutError.name;
  }
}

