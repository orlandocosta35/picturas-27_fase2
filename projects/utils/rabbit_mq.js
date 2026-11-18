const exchange = 'picturas';
const closeConnection = connection => {
    try { connection.close(() => {}); } catch (_) { /* already disconnected */ }
};

// Keep the transport injectable so delivery ordering and disconnects can be
// tested without starting the application or a RabbitMQ broker.
function createRabbitMq(amqp, { url, retryMs = 1000 } = {}) {
    function send_rabbit_msg(msg, queue) {
        const delivery = new Promise((resolve, reject) => {
            let connection;
            let settled = false;
            const finish = (error) => {
                if (settled) return;
                settled = true;
                if (connection) closeConnection(connection);
                if (error) reject(error); else resolve();
            };
            amqp.connect(url, { timeout: 10000 }, (error, conn) => {
                if (error) return finish(error);
                connection = conn;
                conn.on('error', finish);
                conn.on('close', () => finish(new Error('RabbitMQ closed before publication was confirmed')));
                conn.createConfirmChannel((channelError, channel) => {
                    if (channelError) return finish(channelError);
                    channel.on('error', finish);
                    channel.on('close', () => finish(new Error('RabbitMQ channel closed before publication was confirmed')));
                    channel.on('return', () => finish(new Error(`No RabbitMQ queue is bound for ${queue}`)));
                    channel.assertExchange(exchange, 'direct', { durable: true }, (assertError) => {
                        if (assertError) return finish(assertError);
                        channel.publish(exchange, queue, Buffer.from(JSON.stringify(msg)), {
                            persistent: true,
                            mandatory: true,
                            contentType: 'application/json',
                            messageId: msg.messageId,
                        }, finish);
                    });
                });
            });
        });
        // Legacy notification callers do not await this function. Attach a
        // rejection handler while still returning the failure to awaited jobs.
        delivery.catch(error => console.error('[RabbitMQ] publication failed:', error.message));
        return delivery;
    }

    function read_rabbit_msg(queue, callback) {
        let stopped = false;
        let reconnect;
        let connection;
        let handling = Promise.resolve();
        function schedule() {
            if (!stopped && !reconnect) {
                reconnect = setTimeout(() => { reconnect = null; connect(); }, retryMs);
            }
        }
        function connect() {
            if (stopped) return;
            amqp.connect(url, { timeout: 10000 }, (error, conn) => {
                if (error) { schedule(); return; }
                if (stopped) { closeConnection(conn); return; }
                connection = conn;
                conn.on('error', error => console.error('[RabbitMQ] consumer connection:', error.message));
                conn.on('close', schedule);
                conn.createChannel((channelError, channel) => {
                    if (channelError) { closeConnection(conn); return; }
                    let open = true;
                    conn.on('close', () => { open = false; });
                    channel.on('error', error => console.error('[RabbitMQ] consumer channel:', error.message));
                    channel.on('close', () => { open = false; closeConnection(conn); });
                    channel.assertExchange(exchange, 'direct', { durable: true });
                    channel.assertQueue(queue, { durable: true }, (assertError) => {
                        if (assertError) return;
                        channel.prefetch(1);
                        channel.consume(queue, msg => {
                            if (!msg) { closeConnection(conn); return; }
                            // Await persistence before acknowledging, and serialize even
                            // across reconnects so a duplicate sees the completed job.
                            handling = handling.then(async () => {
                                try {
                                    await callback(msg);
                                    if (open) channel.ack(msg);
                                } catch (error) {
                                    console.error('[RabbitMQ] reply handler failed:', error.message);
                                    if (open) {
                                        try { channel.nack(msg, false, true); }
                                        catch (_) { closeConnection(conn); }
                                    }
                                }
                            });
                        }, { noAck: false });
                    });
                });
            });
        }
        connect();
        return () => {
            stopped = true;
            clearTimeout(reconnect);
            if (connection) closeConnection(connection);
        };
    }
    return { send_rabbit_msg, read_rabbit_msg };
}

let transport;
function currentTransport() {
    if (!transport) {
        const host = process.env.RABBITMQ_HOST;
        const port = process.env.RABBITMQ_PORT;
        const user = encodeURIComponent(process.env.RABBITMQ_USER || '');
        const password = encodeURIComponent(process.env.RABBITMQ_PASS || '');
        transport = createRabbitMq(require('amqplib/callback_api'), {
            url: `amqp://${user}:${password}@${host}:${port}`,
        });
    }
    return transport;
}
module.exports = {
    send_rabbit_msg: (...args) => currentTransport().send_rabbit_msg(...args),
    read_rabbit_msg: (...args) => currentTransport().read_rabbit_msg(...args),
    createRabbitMq,
};
