# kafka-pipeline

This library provide a robust, easy to use kafka message consumer based on 
`ConsumerGroup` from [kafka-node] that helps you managing message offsets 
in correct way.

## Design notes

The basic idea behind this library is separate consume progress and offset
commit into two [Transform] stream, namely `ConsumeStream` and 
`CommitStream`, messages will be consumed by `ConsumeStream`
by invoking a specified callback within a specified concurrency limit. And,
`ConsumeStream` will yield all received messages in the same order
to `CommitStream` when all the condition following are met:

1. the message itself has been consumed

2. all predecessor message in the same partition have been consumed

While the `CommitStream` remember the latest offset of each partitions
and topics, commit them repeatedly in a specified interval as well as the time 
when the consumer is rebalanced or closed.


While this idea is originate from 
`ConsumerGroupStream` from [kafka-node], it's buggy when you try to use it
with offset being managed by yourself, which prevents you from implementing
a reliable message consumer meets guarantee that each message should be
consumed at least once.

Even if you don't care about at-least-once guarantee at all, 
`ConsumerGroupStream` duplicates a lot of message after rebalance. Although 
your apps need to be idempotency when handle message, this library take the 
best efforts to minimize the duplication of messages, unless error occurred
that prevent the offset to be committed, it shall NEVER duplicate any message.

## Installation

[kafka-node] is A peer dependency of this library, you need to install it 
together with kafka-pipeline.

```bash
npm install kafka-pipeline kafka-node
```

## Usage

```javascript 
const {ConsumerGroupPipeline} = require('kafka-pipeline');

const consumerPipeline = new ConsumerGroupPipeline({
  topic: TOPIC_NAME,
  messageConsumer: async (message) => {
    //...
  },
  // A optional callback function handled messages failed to consume by 
  // `messageConsumer`. When error raised in `messageConsumer`, 
  // consumerPipeline will be closed if you don't provide `failedMessageConsumer`,
  // or `failedMessageConsumer` also raise an exception 
  failedMessageConsumer: async (exception, message) => {
    //...
  },
  consumeConcurrency: 8,
  consumeTimeout: 5000,
  commitInterval: 10000,
  consumerGroupOption: {
    groupId: GROUP_ID
    // Properties of this object will be pass to ConsumerGroup
    // except that some of them could be override by ConsumerGroupPipeline
    // See reference of its setting at https://github.com/SOHU-Co/kafka-node
    // 
  }
});

consumerPipeline.start();

// The following code is for demo purpose only, not suggesting you to do this
// in production code. But to avoid message duplication you need to ensure your
// app will quit in a graceful way that await the promise returned by 
// consumerPipeline.close() is resolved, which will do the final offset commit
process.on('SIGTERM', function () {
  consumerPipeline.close().then(function () {
    process.exit(0);
  });
});
```

## License

ISC License, see `LICENSE` file.

## TODO

1. More test
2. Consider support HighLevelConsumer?

[kafka-node]: https://github.com/SOHU-Co/kafka-node
[Transform]: https://nodejs.org/api/stream.html#stream_class_stream_transform
