const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { createRabbitMq } = require('../utils/rabbit_mq');
const { saveJobOutput } = require('../utils/jobOutput');

function broker() {
  const connections = [];
  const amqp = {
    connect(_url, _options, callback) {
      const connection = new EventEmitter();
      connection.closed = false;
      connection.close = done => {
        if (!connection.closed) { connection.closed = true; connection.emit('close'); }
        if (done) done();
      };
      const channel = new EventEmitter();
      connection.channel = channel;
      channel.acks = [];
      channel.nacks = [];
      channel.assertExchange = (_name, _type, _options, done) => { if (done) done(); };
      channel.assertQueue = (_name, _options, done) => done();
      channel.prefetch = count => { channel.prefetchCount = count; };
      channel.consume = (_queue, consume) => { channel.deliver = consume; };
      channel.ack = msg => channel.acks.push(msg);
      channel.nack = (...args) => channel.nacks.push(args);
      channel.publish = (_exchange, _key, _body, options, done) => {
        channel.publication = { options, confirm: done };
      };
      connection.createConfirmChannel = done => done(null, channel);
      connection.createChannel = done => done(null, channel);
      connections.push(connection);
      callback(null, connection);
    },
  };
  return { connections, transport: createRabbitMq(amqp, { url: 'test', retryMs: 1 }) };
}

const tick = () => new Promise(resolve => setImmediate(resolve));

test('jobs are persistent and publication resolves only after broker confirmation', async () => {
  const { connections, transport } = broker();
  const delivery = transport.send_rabbit_msg({ messageId: 'job' }, 'video_queue');
  const connection = connections[0];
  const publication = connection.channel.publication;
  assert.equal(publication.options.persistent, true);
  assert.equal(publication.options.mandatory, true);
  assert.equal(connection.closed, false);
  publication.confirm(null);
  await delivery;
  assert.equal(connection.closed, true);
});

test('a disconnect before confirmation rejects the publication', async () => {
  const { connections, transport } = broker();
  const delivery = transport.send_rabbit_msg({ messageId: 'job' }, 'video_queue');
  connections[0].close();
  await assert.rejects(delivery, /before publication was confirmed/);
});

test('an unroutable message is not treated as a successful publication', async () => {
  const { connections, transport } = broker();
  const delivery = transport.send_rabbit_msg({ messageId: 'job' }, 'missing');
  connections[0].channel.emit('return', {});
  await assert.rejects(delivery, /No RabbitMQ queue/);
});

test('duplicate replies cannot run concurrently or acknowledge before persistence', async () => {
  const { connections, transport } = broker();
  const pending = new Set(['job']);
  const saved = [];
  let release;
  const saving = new Promise(resolve => { release = resolve; });
  const stop = transport.read_rabbit_msg('project_queue', async () => {
    if (!pending.has('job')) return;
    await saving;
    saved.push('result');
    pending.delete('job');
  });
  const channel = connections[0].channel;
  const first = { content: Buffer.from('first') };
  const duplicate = { content: Buffer.from('duplicate') };
  channel.deliver(first);
  channel.deliver(duplicate);
  await tick();
  assert.equal(channel.prefetchCount, 1);
  assert.deepEqual(channel.acks, []);
  release();
  await tick();
  assert.deepEqual(saved, ['result']);
  assert.deepEqual(channel.acks, [first, duplicate]);
  stop();
});

test('failed persistence is requeued and consumption recovers after connection loss', async () => {
  const { connections, transport } = broker();
  const stop = transport.read_rabbit_msg('project_queue', async () => { throw new Error('database down'); });
  const channel = connections[0].channel;
  const msg = { content: Buffer.from('reply') };
  channel.deliver(msg);
  await tick();
  assert.deepEqual(channel.acks, []);
  assert.deepEqual(channel.nacks, [[msg, false, true]]);
  connections[0].close();
  await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(connections.length, 2);
  assert.equal(typeof connections[1].channel.deliver, 'function');
  stop();
});

test('repeated video replies upsert one result and distinct extracted frames stay distinct', async () => {
  const rows = new Map();
  const Model = {
    findOneAndUpdate(filter, update, options) {
      return { exec: async () => {
        assert.equal(options.upsert, true);
        if (!rows.has(filter._id)) rows.set(filter._id, { _id: filter._id, ...update.$setOnInsert });
        return rows.get(filter._id);
      } };
    },
  };
  const first = await saveJobOutput(Model, { file_name: 'frame1.png' }, 'job1');
  const duplicate = await saveJobOutput(Model, { file_name: 'frame1.png' }, 'job1');
  await saveJobOutput(Model, { file_name: 'frame2.png' }, 'job1');
  await saveJobOutput(Model, { file_name: 'frame1.png' }, 'job2');
  assert.equal(first._id, duplicate._id);
  assert.equal(rows.size, 3);
});
